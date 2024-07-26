package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
)

func (wContext *workerContext) mapJob(mapF func(string, string) []KeyValue) error {
	//读取文件
	file, err := os.Open(wContext.fileName)
	if err != nil {
		return errors.New(err.Error() + " file name:" + wContext.fileName)
	}
	contentBytes, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	//调用map函数
	kvs := mapF(wContext.fileName, string(contentBytes))

	err = wContext.storeTempFile(kvs)
	if err != nil {
		return err
	}

	return nil
}

// 存储
func (wContext *workerContext) storeTempFile(kvs []KeyValue) error {
	//创建临时文件
	var tempFiles []*os.File
	for i := 0; i < wContext.nReduce; i++ {
		tempFIleName := fmt.Sprintf("mr-map-%d%d", wContext.jobUID, i)
		//在当前目录下创建临时文件
		tempFile, err := os.Create(tempFIleName)
		if err != nil {
			return err
		}
		tempFiles = append(tempFiles, tempFile)
	}

	var tempStore = make([]keyValues, wContext.nReduce)
	for _, v := range kvs {
		idOrigin := iHash(v.Key)
		id := idOrigin % wContext.nReduce

		tempStore[id] = append(tempStore[id], v)
	}

	for i, v := range tempStore {
		//如果v为空，跳过
		if len(v) == 0 {
			//删除空文件
			err := os.Remove(tempFiles[i].Name())
			if err != nil {
				return err
			}

			continue
		}
		sort.Sort(v)
		//json序列化
		jsonData, err := json.Marshal(v)
		if err != nil {
			return err
		}

		//写入文件
		_, err = tempFiles[i].Write(jsonData)
		if err != nil {
			return err
		}
	}

	return nil
}

func iHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
