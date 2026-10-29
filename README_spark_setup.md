# Spark Connect Setup: What Was Installed and How to Remove It

This setup runs **Apache Spark 4.1.2** (with Spark SQL and Spark Connect) on an Ubuntu VPS, and lets PyCharm on a Windows PC send Spark code to it through an encrypted SSH tunnel.

```
PyCharm (.venv)            Windows PC              SSH tunnel             Ubuntu VPS
pyspark-client  ──►  localhost:15002  ══ encrypted ══►  localhost:15002  ──►  Spark Connect server
                                    (ssh sparkvps)                          (systemd: spark-connect)
```

- **VPS:** `YOUR_USER@YOUR_VPS_IP` (Ubuntu, Python 3.12.3)
- **PC:** Windows, user `YOUR_WINDOWS_USER`
- **Spark version:** 4.1.2 (the PC client and VPS server must match)

**Placeholders used in this file** (replace with your real values when following the steps):

| Placeholder | Meaning |
|---|---|
| `YOUR_VPS_IP` | The VPS's public IP address |
| `YOUR_USER` | Your Linux username on the VPS |
| `YOUR_WINDOWS_USER` | Your Windows user folder name (as in `C:\Users\...`) |

---

## 1. What was installed or changed on the VPS

| # | Item | Location / detail |
|---|------|-------------------|
| 1 | System packages updated | `sudo apt update && sudo apt upgrade -y` |
| 2 | Java 17 | apt package `openjdk-17-jdk` → `/usr/lib/jvm/java-17-openjdk-amd64` |
| 3 | Python tools | apt packages `python3-venv`, `python3-pip`, `wget` (`python3` was already part of Ubuntu) |
| 4 | Apache Spark 4.1.2 | `/opt/spark-4.1.2-bin-hadoop3` (includes Spark SQL, PySpark, Spark Connect; logs in `/opt/spark/logs`) |
| 5 | Shortcut link to Spark | `/opt/spark` → `/opt/spark-4.1.2-bin-hadoop3` |
| 6 | Environment variables | 4 lines added to the end of `~/.bashrc` (`JAVA_HOME`, `SPARK_HOME`, `PATH`, `PYSPARK_PYTHON`) |
| 7 | Spark download file | `/tmp/spark-4.1.2-bin-hadoop3.tgz` (about 570 MB; may already be deleted) |
| 8 | Spark SQL test leftovers | `/tmp/metastore_db`, `/tmp/spark-warehouse`, `/tmp/derby.log` (if not already deleted) |
| 9 | Data folder | `~/data` (contains `customers/`, `transactions/`, `generate_csv.py`) |
| 10 | Spark Connect service | `/etc/systemd/system/spark-connect.service`, enabled at boot, listening on port **15002** |
| 11 | Firewall (ufw) | Enabled. Allows **OpenSSH (22)** only. The existing **5432/tcp (PostgreSQL)** rule was **removed**. |
| 12 | PC's SSH key | Public key appended to `~/.ssh/authorized_keys` |

**Not changed on the VPS:** PostgreSQL itself (still installed and running, just firewalled), and the provider's firewall policy (still lists TCP 22, 80, 443, 8443, 8447, 5432).

## 2. What was installed or changed on the Windows PC

| # | Item | Location / detail |
|---|------|-------------------|
| 1 | SSH key pair | `C:\Users\YOUR_WINDOWS_USER\.ssh\id_ed25519` (private, keep secret) and `id_ed25519.pub` (public) |
| 2 | SSH shortcut + tunnel | `C:\Users\YOUR_WINDOWS_USER\.ssh\config`, entry `Host sparkvps` (forwards ports 15002 and 4040) |
| 3 | VPS fingerprint | A line for `YOUR_VPS_IP` in `C:\Users\YOUR_WINDOWS_USER\.ssh\known_hosts` (added on first connection) |
| 4 | PyCharm virtualenv | `C:\Users\YOUR_WINDOWS_USER\PycharmProjects\Coursera\PySparkCustomer\.venv` (Python 3.13) |
| 5 | Spark Connect client | `pyspark-client==4.1.2` and its dependencies (such as `grpcio`, `protobuf`), inside that `.venv` only |
| 6 | Test script | `PySparkCustomer\test_spark.py` |

**Not yet done:** DBeaver's SSH tunnel for PostgreSQL (see section 5).

---

## 3. Daily use

1. Open PowerShell and run `ssh sparkvps`. **Leave it open**, because this is the tunnel.
2. In PyCharm, run scripts that start with:
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
   ```
3. Data paths in `spark.read...` refer to the **VPS**, for example `/home/YOUR_USER/data/customers`.
4. Upload data from the PC:
   ```powershell
   scp -r C:\path\to\folder sparkvps:~/data/
   scp C:\path\to\file.csv sparkvps:~/data/
   ```
5. Spark job monitor (while a job runs): `http://localhost:4040`
6. Spark SQL shell: in the `ssh sparkvps` window, run `spark-sql`.

**Useful VPS commands**

```bash
sudo systemctl status spark-connect     # is the server running?
sudo systemctl restart spark-connect    # restart it
tail -50 /opt/spark/logs/*Connect*.out  # server logs
sudo ufw status                         # firewall rules
```

**New project on the PC:** create a venv, run `pip install pyspark-client==4.1.2`, and use `.remote("sc://localhost:15002")`.

---

## 4. Troubleshooting

| Problem | Fix |
|---|---|
| `Connection refused` on 15002 | The tunnel isn't open (run `ssh sparkvps`), or the server is down (`sudo systemctl status spark-connect`). |
| `Could not resolve hostname sparkvps` | `C:\Users\YOUR_WINDOWS_USER\.ssh\config` is missing or was saved as `config.txt`. |
| Version mismatch error | The `pyspark-client` version must equal the VPS Spark version (4.1.2). |
| `Path does not exist` | The file must be on the VPS. Use `scp` and a full VPS path. |
| Out of memory | Lower `spark.driver.memory` in the service file, then `sudo systemctl daemon-reload && sudo systemctl restart spark-connect`. |
| "Port already in use" warning on `ssh sparkvps` | Another `ssh sparkvps` window already holds the tunnel. Harmless. |

---

## 5. DBeaver → PostgreSQL (to do)

Port 5432 is blocked, so DBeaver must connect through SSH:

- **Main tab:** Host `localhost`, Port `5432`, with the same database, username and password as before.
- **SSH tab:** tick **Use SSH Tunnel**, Host `YOUR_VPS_IP`, Port `22`, User `YOUR_USER`, Authentication **Public Key**, Private key `C:\Users\YOUR_WINDOWS_USER\.ssh\id_ed25519`.

---

## 6. How to uninstall

Do only the parts you want to remove. Section A on its own fully stops Spark from running.

### A. VPS: stop and remove the Spark Connect service

```bash
sudo systemctl disable --now spark-connect
sudo rm /etc/systemd/system/spark-connect.service
sudo systemctl daemon-reload
```

### B. VPS: remove Spark

```bash
sudo rm -rf /opt/spark-4.1.2-bin-hadoop3
sudo rm /opt/spark
rm -f /tmp/spark-4.1.2-bin-hadoop3.tgz
rm -rf /tmp/metastore_db /tmp/spark-warehouse /tmp/derby.log
rm -rf ~/metastore_db ~/spark-warehouse ~/derby.log   # in case spark-sql was run from home
```

### C. VPS: remove the environment variables

```bash
nano ~/.bashrc
```

Delete these four lines at the bottom, then save (**Ctrl+O**, **Enter**, **Ctrl+X**):

```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3
```

Then run `source ~/.bashrc` (or log out and back in).

### D. VPS: remove Java (only if nothing else needs it)

```bash
sudo apt remove --purge -y openjdk-17-jdk openjdk-17-jre openjdk-17-jdk-headless openjdk-17-jre-headless
sudo apt autoremove -y
```

⚠️ **Do not remove `python3`.** Ubuntu itself depends on it. `python3-venv`, `python3-pip` and `wget` are harmless to keep.

### E. VPS: remove the uploaded data (optional)

```bash
rm -rf ~/data
```

### F. VPS: firewall (optional)

The firewall only allowing SSH is good security, so it's recommended to **keep it**. To undo changes:

```bash
sudo ufw allow 5432/tcp     # re-open PostgreSQL to the internet (as before)
sudo ufw disable            # or turn the firewall off completely
```

### G. PC: remove the Spark client from the project

Either delete the whole virtualenv by closing PyCharm and deleting the folder:

```
C:\Users\YOUR_WINDOWS_USER\PycharmProjects\Coursera\PySparkCustomer\.venv
```

or just uninstall the package in PyCharm's Terminal:

```powershell
pip uninstall -y pyspark-client
```

Delete `test_spark.py` if you no longer need it.

### H. PC: remove the SSH shortcut

```powershell
notepad "$env:USERPROFILE\.ssh\config"
```

Delete the whole `Host sparkvps` block (7 lines) and save. If it was the only entry, you can delete the file:

```powershell
Remove-Item "$env:USERPROFILE\.ssh\config"
```

### I. SSH key (only if you want to stop key login completely)

1. On the VPS, open `~/.ssh/authorized_keys` and delete the line for this PC's key:
   ```bash
   nano ~/.ssh/authorized_keys
   ```
2. On the PC:
   ```powershell
   Remove-Item "$env:USERPROFILE\.ssh\id_ed25519", "$env:USERPROFILE\.ssh\id_ed25519.pub"
   ```

⚠️ If the VPS ever has password login disabled, removing the key could lock you out. In that case, use your provider's web console to get back in.
