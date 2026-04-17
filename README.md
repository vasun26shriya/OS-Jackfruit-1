# Multi-Container Runtime

This project implements a lightweight container runtime in C with a long-running supervisor and a kernel memory monitor.

## 1. Team Information

Name 1: Shriya Vasundhara Singh
SRN 1: PES1UG24AM269
Name 2: Shreya Yashodhara Singh
SRN 2: PES1UG24AM266

---

## 2. Build, Load, Run, and Cleanup Instructions

### 2.1 Environment Prerequisites

- Ubuntu 22.04 or 24.04 VM
- Secure Boot disabled
- Not supported on WSL

Install packages:

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

Optional preflight:

```bash
cd boilerplate
chmod +x environment-check.sh
sudo ./environment-check.sh
```

### 2.2 Prepare Root Filesystem

Run from repository root:

```bash
mkdir -p rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base

cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
```

### 2.3 Build

```bash
cd boilerplate
make clean
make
```

For CI-safe compile checks:

```bash
make ci
```

### 2.4 Load Monitor Module

```bash
sudo insmod monitor.ko
ls -l /dev/container_monitor
```

If needed for local testing:

```bash
sudo chmod 666 /dev/container_monitor
```

### 2.5 Start Supervisor

```bash
sudo ./engine supervisor ./rootfs-base
```

### 2.6 Container Operations (in another terminal)

Start containers in background:

```bash
sudo ./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80 --nice -5
sudo ./engine start beta ./rootfs-beta /bin/sh --soft-mib 64 --hard-mib 96 --nice 10
```

Foreground run (waits for completion):

```bash
sudo ./engine run gamma ./rootfs-alpha /bin/sh --soft-mib 40 --hard-mib 64 --nice 0
```

Inspect runtime state:

```bash
sudo ./engine ps
sudo ./engine logs alpha
```

Stop running containers:

```bash
sudo ./engine stop alpha
sudo ./engine stop beta
```

### 2.7 Running Workloads for Memory/Scheduling Tests

Copy workload binaries into container rootfs before launching test containers:

```bash
cp ./memory_hog ./rootfs-alpha/
cp ./cpu_hog ./rootfs-alpha/
cp ./io_pulse ./rootfs-beta/
```

Example test commands inside containers use `/memory_hog`, `/cpu_hog`, `/io_pulse`.

### 2.8 Cleanup

```bash
Ctrl + C
ps aux | grep -E 'defunct|engine'
sudo rmmod monitor
make clean
```

---

## 3. Demo Screenshots

### SS1. Supervisor startup + multi-container launch
![Supervisor startup]![alt text](1-2.png)
![Multiple containers]![alt text](2.png)
Supervisor starts and then tracks multiple containers concurrently.

### SS2. Metadata tracking (`engine ps`)
![Metadata table]![alt text](3.png)
Container ID, host PID, state, and limits are visible in one listing.

### SS3. Logging pipeline
![Pipeline output]![alt text](4.png)
Logs captured from container stdout/stderr are persisted and retrievable.

### SS4. CLI to supervisor IPC
![CLI IPC]![alt text](5.png)
CLI client commands are issued from a separate terminal, and the supervisor returns a structured response over the Unix domain socket control channel.

### SS5. Soft-limit warning event
![Soft-limit event]![alt text](6.png)
`dmesg` records first threshold crossing at soft memory limit.

### SS6. Hard-limit enforcement
![Hard-limit event]![alt text](7.png)
Container state shows kill outcome after exceeding configured limits.

### SS7. Scheduling experiment evidence
![Scheduling evidence]![alt text](8.png)
Different `nice` values produce visible CPU-share differences.

### SS8. Clean teardown
![Teardown]![alt text](9.png)
No leftover supervisor/container zombies after shutdown.

---

## 4. Engineering Analysis

### 4.1 Isolation Mechanisms

Isolation is achieved by combining Linux namespaces with rootfs isolation.

- PID namespace (`CLONE_NEWPID`) gives a private process tree to each container.
- UTS namespace (`CLONE_NEWUTS`) allows per-container hostname separation.
- Mount namespace (`CLONE_NEWNS`) prevents mount operations from leaking to host.
- Filesystem isolation is applied using `chroot`/`pivot_root` into dedicated rootfs copies.

Even with namespace isolation, containers still share the host kernel, which is why kernel-level enforcement remains critical for safety boundaries.

### 4.2 Supervisor and Process Lifecycle

The long-running supervisor is the lifecycle authority for all containers.

- It creates container children and records metadata at launch time.
- It handles `SIGCHLD` and uses `waitpid()` to reap exits and avoid zombies.
- It tracks terminal state (`running`, `stopped`, `killed`, `exited`) plus exit status/signal.

This pattern mirrors an init-like responsibility for container descendants.

### 4.3 IPC, Threads, and Synchronization

The design uses separate channels for control and data:

- CLI to supervisor control path: Unix domain socket
- Container stdout/stderr to supervisor: pipes
- Supervisor to kernel monitor: `ioctl` (`/dev/container_monitor`)

Logging uses a bounded producer-consumer queue protected by mutex/condition variables. Without synchronization, concurrent writers could corrupt shared indices and lose log records. Bounded capacity also prevents unbounded memory growth during bursts.

### 4.4 Memory Management and Enforcement

RSS(Resident set size) indicates resident physical memory pages for a process but does not capture every form of virtual reservation. The runtime applies two-level policy:

- Soft limit: warning event for observability
- Hard limit: forced termination (`SIGKILL`) for host protection

Kernel-space enforcement is chosen because it remains effective even when user-space is delayed, blocked, or starved.

### 4.5 Scheduling Behavior

Experiments with differing nice levels show CFS weight differences in practice. Lower nice values receive more CPU share under contention, while higher nice workloads complete more slowly but still make progress. The outcomes illustrate CFS trade-offs between fairness and responsiveness.

---

## 5. Design Decisions and Challenges

- Control IPC was implemented with Unix sockets for simple request/response semantics and robust local delivery.
- Logging used bounded buffering to avoid contention between fast producers and slower file consumers.
- Memory monitoring was delegated to the kernel module to guarantee hard-limit action.
- Key debugging effort: ensuring cleanup order (stop signal, reap, unregister, logger drain) to avoid stale state.

---

## 6. Scheduling Experiment Results

Two CPU-intensive workloads were started concurrently with different priorities.

| Container | Nice Value | Observed Behavior |
| :-- | :-- | :-- |
| Alpha (hipri) | -5 | Received significantly higher CPU share (~99–100% in pidstat) and completed `cpu_hog` faster |
| Beta (lopri) | 10 | Received lower CPU share (~85–96% in pidstat) and showed slower progress under contention |

Interpretation: CFS did not starve the lower-priority task completely, but weighted virtual runtime strongly favored the higher-priority process. These observations are consistent with the pidstat output shown in SS7.

---

## 7. Final Ubuntu Verification Checklist

Run this once in your Ubuntu VM before submission.

```bash
# 0) Clean and rebuild
cd boilerplate
make clean
make
make ci

# 1) Load monitor and verify device
sudo insmod monitor.ko
ls -l /dev/container_monitor

# 2) Start supervisor (terminal A)
sudo ./engine supervisor ./rootfs-base

# 3) Prepare writable rootfs copies (terminal B)
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta

# 4) Start two containers (terminal B)
sudo ./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80 --nice -5
sudo ./engine start beta  ./rootfs-beta  /bin/sh --soft-mib 64 --hard-mib 96 --nice 10

# 5) Verify metadata and logs
sudo ./engine ps
sudo ./engine logs alpha

# 6) Run scheduling/memory evidence commands as needed
# (run workload binaries copied into rootfs and capture screenshots)

# 7) Stop containers and confirm cleanup
sudo ./engine stop alpha
sudo ./engine stop beta
ps aux | grep -E 'defunct|engine'

# 8) Inspect kernel events and unload module
dmesg | tail -n 20
sudo rmmod monitor
```

Pass criteria:

- `make`, `make ci`, and module load succeed without errors.
- `engine ps` shows tracked containers and final states correctly.
- Soft-limit and hard-limit events appear in `dmesg` during tests.
- No lingering zombie processes after stop/teardown.

---