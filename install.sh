# https://www.youtube.com/watch?v=H999fIuymqc
# https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

if ! command -v java &>/dev/null; then echo "java is not installed"; exit 1; fi

brew install hadoop

