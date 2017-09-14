package main

import (
	"fmt"
	"math/rand"
	"testing"

	ObsClient "com/client"
	"com/models"

	"github.com/satori/go.uuid"
)

type Obs struct {
	Client *ObsClient.Client
}

const (
	BUCKET_NAME = "bentchmark"
	AK          = ""
	SK          = ""
	REGION      = ""
	SVR         = ""
	AUTH        = ""
)

var (
	OBS       *Obs
	FILE_NAME []string
)

func init() {
	OBS = NewObs(AK, SK, REGION, AUTH, SVR)
	FILE_NAME = []string{"lt1k", "lt100k", "lt1m", "lt10m", "lt20m"}
}

func NewObs(ak, sk, reg, auth, svr string) *Obs {
	obs := new(Obs)
	obs.Client = ObsClient.FactoryEx(ak, sk, reg, auth, svr, true)

	return obs
}

func (obs Obs) putObject(bucket, fileName string) {
	input := new(models.PutObjectInput)
	input.Bucket = bucket
	input.Object = fmt.Sprintf("%s-%s", fileName, uuid.NewV4())
	input.ACL = models.PUBLIC_READ_WRITE
	input.SourceFile = fileName
	requst, output := obs.Client.PutObject(input)
	fmt.Printf("err:%s,statusCode:%d,code:%s,message:%s\n", requst.Err, requst.StatusCode, requst.Code, requst.Message)
	if output != nil {
		fmt.Printf("ETag:%s,VersionId:%s\n", output.ETag, output.VersionId)
	}
}

func (obs Obs) listObjects(bucket string) []string {
	objs := []string{}
	marker := ""
	for {
		input := new(models.ListObjectsInput)
		input.Bucket = bucket
		input.Marker = marker
		requst, output := obs.Client.ListObjects(input)
		fmt.Printf("err:%s,statusCode:%d,code:%s,message:%s\n", requst.Err, requst.StatusCode, requst.Code, requst.Message)
		if output != nil {
			for _, val := range output.Contents {
				objs = append(objs, val.Key)
			}
		}
		marker = output.NextMarker
		if marker == "" {
			break
		}
	}

	return objs
}

func (obs Obs) delObjects(bucket string, objs []string) {
	input := new(models.DeleteObjectsInput)
	input.Bucket = bucket
	input.Quiet = false
	keys := make([]models.Object, len(objs))
	for i, obj := range objs {
		keys[i].Object = obj
	}
	input.Objects = append(input.Objects, keys...)
	requst, _ := obs.Client.DeleteObjects(input)
	fmt.Printf("err:%s,statusCode:%d,code:%s,message:%s\n", requst.Err, requst.StatusCode, requst.Code, requst.Message)
}

func (obs Obs) clearBucket(bucket string) {
	objs := obs.listObjects(bucket)
	obs.delObjects(bucket, objs)
}

func Bentchmark_LT1K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		OBS.putObject(BUCKET_NAME, "lt1k")
	}
}

func Bentchmark_LT100K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		OBS.putObject(BUCKET_NAME, "lt100k")
	}
}

func Bentchmark_LT1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		OBS.putObject(BUCKET_NAME, "lt1m")
	}
}

func Bentchmark_LT10M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		OBS.putObject(BUCKET_NAME, "lt10m")
	}
}

func Bentchmark_LT20M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		OBS.putObject(BUCKET_NAME, "lt20m")
	}
}

func Bentchmark_multiPut(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		obs := NewObs(AK, SK, REGION, AUTH, SVR)
		for pb.Next() {
			fileName := FILE_NAME[rand.Intn(4)]
			obs.putObject(BUCKET_NAME, fileName)
		}
	})
}
