package mapreduce

import (
	"hash/fnv"
	"os"
	"log"
	"encoding/json"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	/*
		1. 打开输入文件
		2. 输入到Map中
		3. 生成nReduce个中间文件
	 */
	file, err := os.Open(inFile)
	//fmt.Printf("Read filename: %s, MapTaskNumber: %d, nReduce: %d, jobName: %s\n", inFile, mapTaskNumber, nReduce, jobName)
	if err != nil {
		log.Fatal("Open file error: ", err)
	}
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal("Get file info error: ", err)
	}
	fileSize := fileInfo.Size()
	buf := make([]byte, fileSize)
	_, err = file.Read(buf)
	//fmt.Printf(string(buf))
	if err != nil {
		log.Fatal("Read error: ", err)
	}
	res := mapF(inFile, string(buf)) // res []KeyValue 键值对数组
	rSize := len(res)
	//fmt.Printf("the Map res size: %d\n", rSize)
	file.Close()
	for i := 0; i < nReduce; i++ {  // 生成nReduce个中间文件
		fileName := reduceName(jobName, mapTaskNumber, i)
		//fmt.Printf("Debug: Map filename: %s\n", fileName)
		file, err := os.Create(fileName)  // 创建新中间文件
		if err != nil {
			log.Fatal("Create mid file: ", err)
		}
		enc := json.NewEncoder(file)
		for r := 0; r < rSize; r++ {
			kv := res[r]
			// 将对应key的值通过hash存储在对应的nReduce个文件中(也就是找到key应该映射到那个文件中)
			if ihash(kv.Key) % uint32(nReduce) == uint32(i) {
				//fmt.Printf("save data: %s ", kv)
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("Encode error: ", kv)
				}
			}
		}
		file.Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
