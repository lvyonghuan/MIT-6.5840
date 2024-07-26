package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

func (wContext *workerContext) reduceJob(reduceF func(string, []string) string) error {
	//读取文件并洗牌
	kvs, err := wContext.reduceGetMapFile()
	if err != nil {
		return err
	}
	shuffleResult := wContext.reduceShuffle(kvs)

	var results keyValues
	//调用reduce函数
	for k, v := range shuffleResult {
		result := reduceF(k, v)
		results = append(results, KeyValue{
			Key:   k,
			Value: result,
		})
	}

	//存储结果
	err = wContext.storeResult(results)
	if err != nil {
		return err
	}

	return nil
}

// 读取文件并返回kv切片
func (wContext *workerContext) reduceGetMapFile() (keyValues, error) {
	fileName := "mr-map-*" + strconv.Itoa(wContext.jobUID)
	files, err := filepath.Glob(fileName)
	if err != nil {
		return nil, err
	}

	var kvs keyValues

	for _, v := range files {
		//打开文件
		file, err := os.Open(v)
		if err != nil {
			return nil, err
		}

		// 读取文件
		contentBytes, err := io.ReadAll(file)
		if err != nil {
			return nil, err
		}

		// 反序列化
		var tempKvs keyValues
		err = json.Unmarshal(contentBytes, &tempKvs)
		if err != nil {
			//log.Println(string(contentBytes))
			return nil, err
		}
		kvs = append(kvs, tempKvs...)
	}

	return kvs, nil
}

// 洗牌并返回map[string][]string
func (wContext *workerContext) reduceShuffle(kvs keyValues) map[string][]string {
	shuffleResult := make(map[string][]string)
	for _, v := range kvs {
		shuffleResult[v.Key] = append(shuffleResult[v.Key], v.Value)
	}

	return shuffleResult
}

// 存储结果
func (wContext *workerContext) storeResult(results keyValues) error {
	fileName := "mr-out-" + strconv.Itoa(wContext.jobUID)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	for _, v := range results {
		_, err = file.WriteString(fmt.Sprintf("%v %v", v.Key, v.Value) + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}
