
set -o errexit
cd sim
echo "切换到目录sim... $(pwd)"
config_path="configs/config.json"
config="../${config_path}"
echo "移除log文件..."
if [ -f out.log ]; then
  rm out.log
fi
time_format=$(date +"%d_%H-%M-%S")
echo "当前时间： $time_format"
start_time=$(date +%s)
echo "模拟开始"
python3 simulation.py "$config" "$time_format" -d
end_time=$(date +%s)

echo "当前时间: $(date +"%H:%M:%S") 总共用时： $(( end_time - start_time )) 秒, 约 $(( (end_time - start_time) / 60 )) 分"

cd ..

echo ""
echo "执行分析过程..."
filename="DARC_$time_format"
echo "文件名: $filename"

python3 analysis.py "${filename}" "${filename}" 10
Python3 records.py "$config_path" "${filename}" 10
