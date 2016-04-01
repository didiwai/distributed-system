package mapreduce

import (
	"os"
	"encoding/json"
	"log"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	/*
		该函数收集由`map`产生的nReduce文件(f-*-), 然后对这些写文件运行reduce函数. 最终产生nReduce个结果文件
		1. 从nMap个中间文件中解码Json数据, 使用map收集数据
		2. 对key-values进行排序
		3. 调用reduce函数
		4. 最后生成nReduce个结果文件
	 */
	keyValues := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		//fmt.Printf("Reduce fileName: %s\n", fileName)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("Open Error: ", fileName)
		}
		// func NewDecoder(r io.Reader) *Decoder
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break  // 此时文件解码完毕
			}
			_, ok := keyValues[kv.Key]
			if !ok { // 说明当前并没有这个key
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
		file.Close()
	}
	//fmt.Println("hello world")

	var keys []string
	for k, _ := range keyValues {
		//fmt.Printf("key: %s, ", k)
		keys = append(keys, k)
	}
	sort.Strings(keys)  // 递增排序
	file, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil {
		log.Fatal("Create file error: ", err)
	}
	enc := json.NewEncoder(file)
	for _, k := range keys {
		res := reduceF(k, keyValues[k])
		enc.Encode(KeyValue{k, res})
	}
	file.Close()
}
