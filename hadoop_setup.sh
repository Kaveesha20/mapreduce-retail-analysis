#!/usr/bin/env bash

set -euo pipefail

IMAGE="apache/hadoop:3"
CONTAINER="hadoop-mr"
HDFS_INPUT="/user/student/data"
HDFS_OUTPUT="/user/student/output"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; RESET='\033[0m'

step() { echo -e "\n${BOLD}${CYAN}┌─ Step $1/8: $2${RESET}"; }
ok()   { echo -e "  ${GREEN}✓${RESET}  $1"; }
info() { echo -e "  ${DIM}→${RESET}  $1"; }
warn() { echo -e "  ${YELLOW}⚠${RESET}  $1"; }
fail() { echo -e "\n${RED}✗ FATAL:${RESET} $1" >&2; exit 1; }

hx()  { docker exec -u hadoop "$CONTAINER" bash -c "$*"; }
hxr() { docker exec -u root   "$CONTAINER" bash -c "$*"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
  cat <<EOF
Usage: bash hadoop_setup.sh [options]

Starts a single-node Hadoop cluster in Docker and runs a streaming MapReduce job.

Options:
  --image NAME        Docker image to use (default: $IMAGE)
  --container NAME    Docker container name (default: $CONTAINER)
  --hdfs-input PATH   HDFS input directory (default: $HDFS_INPUT)
  --hdfs-output PATH  HDFS output directory (default: $HDFS_OUTPUT)
  -h, --help          Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; usage; exit 1; }
      IMAGE="$2"; shift 2 ;;
    --container)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; usage; exit 1; }
      CONTAINER="$2"; shift 2 ;;
    --hdfs-input)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; usage; exit 1; }
      HDFS_INPUT="$2"; shift 2 ;;
    --hdfs-output)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; usage; exit 1; }
      HDFS_OUTPUT="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1 ;;
  esac
done

command -v docker &>/dev/null || fail "Docker not found."
for f in Retail_Transactions_Dataset.csv mapper.py reducer.py; do
  [[ -f "$f" ]] || fail "Required file not found: $f"
done

echo -e "\n${BOLD}${CYAN}╔══════════════════════════════════════════════════════╗"
echo -e "║   Hadoop Streaming MapReduce — Docker Setup          ║"
echo -e "║   Dataset: Retail Transactions (1,000,000 rows)      ║"
echo -e "╚══════════════════════════════════════════════════════╝${RESET}"

step 1 "Pull Docker image"
docker pull "$IMAGE"
ok "Image ready: $IMAGE"

step 2 "Start container"
if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  warn "Removing existing container '$CONTAINER'..."
  docker rm -f "$CONTAINER" >/dev/null
fi
docker run -d --name "$CONTAINER" --hostname namenode \
  -p 9870:9870 -p 8088:8088 -p 9864:9864 \
  "$IMAGE" sleep infinity
ok "Container started"

step 3 "Copy files into container"
info "Copying dataset (may take 30s)..."
docker cp Retail_Transactions_Dataset.csv "$CONTAINER:/tmp/Retail_Transactions_Dataset.csv"
ok "Copied dataset"
docker cp mapper.py  "$CONTAINER:/tmp/mapper.py"
docker cp reducer.py "$CONTAINER:/tmp/reducer.py"
ok "Copied mapper.py and reducer.py"

info "Fixing CentOS 7 yum repos (EOL)..."
hxr "sed -i 's/mirror.centos.org/vault.centos.org/g' /etc/yum.repos.d/*.repo && \
     sed -i 's|^mirrorlist=|#mirrorlist=|g' /etc/yum.repos.d/*.repo && \
     sed -i 's|^#baseurl=|baseurl=|g' /etc/yum.repos.d/*.repo"
ok "Repos fixed"
info "Installing Python 3..."
hxr "yum install -y python3 2>&1 | tail -2"
ok "Python 3 ready: $(hx 'python3 --version 2>&1')"

step 4 "Configure Hadoop and start HDFS"
TMPCONF=$(mktemp -d)
trap 'rm -rf "$TMPCONF"' EXIT

cat > "$TMPCONF/core-site.xml" << 'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property>
</configuration>
XMLEOF

cat > "$TMPCONF/hdfs-site.xml" << 'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///tmp/hadoop-root/dfs/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///tmp/hadoop-root/dfs/data</value></property>
</configuration>
XMLEOF

cat > "$TMPCONF/mapred-site.xml" << 'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>mapreduce.application.classpath</name><value>/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/mapreduce/lib/*</value></property>
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
</configuration>
XMLEOF

cat > "$TMPCONF/yarn-site.xml" << 'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>namenode</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.nodemanager.env-whitelist</name><value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value></property>
</configuration>
XMLEOF

for xml in core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml; do
  docker cp "$TMPCONF/$xml" "$CONTAINER:/opt/hadoop/etc/hadoop/$xml"
  ok "Wrote $xml"
done

hx "hdfs namenode -format -force -nonInteractive 2>&1 | grep -E '(formatted|ERROR)' | head -2" || true
hx "hdfs --daemon start namenode"
hx "hdfs --daemon start datanode"
hx "hdfs dfsadmin -safemode wait 2>&1 | tail -1"
ok "HDFS ready"

step 5 "Start YARN"
hx "yarn --daemon start resourcemanager"
hx "yarn --daemon start nodemanager"
info "Waiting for NodeManager (up to 60s)..."
yarn_ready=0
for i in $(seq 1 30); do
  if hx "yarn node -list 2>/dev/null" | grep -q "RUNNING"; then
    yarn_ready=1
    ok "YARN ready"
    break
  fi
  sleep 2
done
[[ "$yarn_ready" -eq 1 ]] || fail "YARN NodeManager did not become RUNNING in time."

step 6 "Upload dataset to HDFS"
hx "hdfs dfs -mkdir -p $HDFS_INPUT"
info "Uploading dataset (may take 60s)..."
hx "hdfs dfs -put -f /tmp/Retail_Transactions_Dataset.csv $HDFS_INPUT/"
ok "Uploaded to hdfs:$HDFS_INPUT/"
hx "hdfs dfs -ls $HDFS_INPUT"

step 7 "Run Hadoop Streaming MapReduce job"
STREAMING_JAR=$(docker exec "$CONTAINER" bash -c \
  "find /opt/hadoop/share/hadoop/tools/lib -name 'hadoop-streaming*.jar' | head -1")
[[ -z "$STREAMING_JAR" ]] && fail "hadoop-streaming jar not found."
ok "Jar: $STREAMING_JAR"
hx "hdfs dfs -rm -r -f $HDFS_OUTPUT 2>/dev/null; true"

echo -e "\n  ${DIM}input   → hdfs:$HDFS_INPUT/Retail_Transactions_Dataset.csv${RESET}"
echo -e "  ${DIM}output  → hdfs:$HDFS_OUTPUT${RESET}\n"

hx "hadoop jar $STREAMING_JAR \
    -files /tmp/mapper.py,/tmp/reducer.py \
    -input  $HDFS_INPUT/Retail_Transactions_Dataset.csv \
    -output $HDFS_OUTPUT \
    -mapper  'python3 mapper.py' \
    -reducer 'python3 reducer.py'"
ok "Job completed successfully"

step 8 "Display output from HDFS"
echo ""
hx "hdfs dfs -cat $HDFS_OUTPUT/part-* 2>/dev/null" | sort -t$'\t' -k2 -rn | column -t -s $'\t'
echo ""
ok "HDFS NameNode UI → http://localhost:9870"
ok "YARN UI          → http://localhost:8088"
echo -e "\n  ${DIM}Stop:   docker stop $CONTAINER${RESET}"
echo -e "  ${DIM}Remove: docker rm -f $CONTAINER${RESET}\n"