/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Full implementation:
 *   - Linked list of tracked container entries (with mutex protection)
 *   - Periodic timer polls RSS of each tracked PID every second
 *   - Soft-limit: emits a one-time kernel warning
 *   - Hard-limit: sends SIGKILL and removes entry
 *   - ioctl: MONITOR_REGISTER / MONITOR_UNREGISTER
 *   - Module init/exit with full cleanup
 *
 * Mutex choice justification:
 *   The timer callback runs in a softirq context but we use a work queue
 *   deferral pattern (via a flag) — actually, timer callbacks on modern
 *   kernels fire in the softirq context where sleeping is NOT allowed.
 *   Therefore we use a SPINLOCK (not a mutex) for the list, since:
 *     - Spinlocks are safe in interrupt/softirq context
 *     - Our critical sections are short (list traversal + alloc/free outside lock)
 *     - We never sleep while holding the lock
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME        "container_monitor"
#define CHECK_INTERVAL_SEC 1

/* ---------------------------------------------------------------
 * TODO 1: Monitored entry struct
 * --------------------------------------------------------------- */
struct monitored_entry {
    pid_t pid;
    char  container_id[MONITOR_NAME_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int soft_warned;            /* have we emitted the soft-limit warning already? */
    struct list_head list;      /* kernel list linkage */
};

/* ---------------------------------------------------------------
 * TODO 2: Global list + spinlock
 *
 * We use a spinlock because the timer callback runs in softirq context
 * where sleeping (mutex) is forbidden.
 * --------------------------------------------------------------- */
static LIST_HEAD(monitored_list);
static DEFINE_SPINLOCK(monitor_lock);

/* --- Provided: internal device / timer state --- */
static struct timer_list monitor_timer;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ---------------------------------------------------------------
 * Provided: soft-limit event helper
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: hard-limit event helper
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu — killed\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * TODO 3: Timer Callback — periodic RSS monitoring
 * --------------------------------------------------------------- */
static void timer_callback(struct timer_list *t)
{
    struct monitored_entry *entry, *tmp;
    /* We collect entries to remove outside the spinlock to avoid
     * calling kfree while holding the spinlock. */
    LIST_HEAD(to_remove);

    spin_lock(&monitor_lock);

    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        long rss = get_rss_bytes(entry->pid);

        if (rss < 0) {
            /* Process is gone — schedule for removal */
            list_del(&entry->list);
            list_add(&entry->list, &to_remove);
            continue;
        }

        /* Hard limit check */
        if (entry->hard_limit_bytes > 0 &&
            (unsigned long)rss > entry->hard_limit_bytes) {
            kill_process(entry->container_id, entry->pid,
                         entry->hard_limit_bytes, rss);
            list_del(&entry->list);
            list_add(&entry->list, &to_remove);
            continue;
        }

        /* Soft limit check — warn only once */
        if (!entry->soft_warned &&
            entry->soft_limit_bytes > 0 &&
            (unsigned long)rss > entry->soft_limit_bytes) {
            log_soft_limit_event(entry->container_id, entry->pid,
                                  entry->soft_limit_bytes, rss);
            entry->soft_warned = 1;
        }
    }

    spin_unlock(&monitor_lock);

    /* Free removed entries outside the spinlock */
    list_for_each_entry_safe(entry, tmp, &to_remove, list) {
        list_del(&entry->list);
        kfree(entry);
    }

    /* Reschedule */
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ---------------------------------------------------------------
 * IOCTL Handler
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;
    struct monitored_entry *entry, *tmp;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    /* Null-terminate just in case */
    req.container_id[MONITOR_NAME_LEN - 1] = '\0';

    if (cmd == MONITOR_REGISTER) {
        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid,
               req.soft_limit_bytes, req.hard_limit_bytes);

        /* TODO 4: Allocate and insert entry */
        if (req.soft_limit_bytes > req.hard_limit_bytes && req.hard_limit_bytes != 0) {
            printk(KERN_WARNING "[container_monitor] Invalid limits: soft > hard\n");
            return -EINVAL;
        }

        entry = kzalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;

        entry->pid = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_warned = 0;
        strncpy(entry->container_id, req.container_id, MONITOR_NAME_LEN - 1);
        INIT_LIST_HEAD(&entry->list);

        spin_lock(&monitor_lock);
        list_add_tail(&entry->list, &monitored_list);
        spin_unlock(&monitor_lock);

        return 0;
    }

    /* MONITOR_UNREGISTER */
    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    /* TODO 5: Remove matching entry */
    spin_lock(&monitor_lock);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        if (entry->pid == req.pid &&
            strncmp(entry->container_id, req.container_id, MONITOR_NAME_LEN) == 0) {
            list_del(&entry->list);
            spin_unlock(&monitor_lock);
            kfree(entry);
            return 0;
        }
    }
    spin_unlock(&monitor_lock);

    return -ENOENT;
}

/* --- Provided: file operations --- */
static struct file_operations fops = {
    .owner          = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void)
{
    struct monitored_entry *entry, *tmp;

    del_timer_sync(&monitor_timer);

    /* TODO 6: Free all remaining monitored entries */
    spin_lock(&monitor_lock);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }
    spin_unlock(&monitor_lock);

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
