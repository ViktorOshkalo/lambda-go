package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	s3Client *s3.Client
)

func init() {
	// Initialize the S3 client outside of the handler, during the init phase
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	s3Client = s3.NewFromConfig(cfg)
}

type Params2 struct {
	Records []struct {
		S3 struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func handler(ctx context.Context, event Params2) (float64, error) {
	log.Printf("triggered by event: %v\n", event)

	bucket := event.Records[0].S3.Bucket.Name
	object := event.Records[0].S3.Object.Key

	log.Printf("processing starting with params: bucket name: %s, file name: %s",
		bucket,
		object)

	obj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})

	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Printf("can't get object %s from bucket %s. no such key exists.\n", object, bucket)
			err = noKey
		} else {
			log.Printf("couldn't get object %v:%v: %v\n", bucket, object, err)
		}
		return 0, err
	}

	defer obj.Body.Close()

	result, err := CalculateGPAAverage(obj.Body)
	if err != nil {
		return 0, err
	}

	log.Printf("average GPA: %f\n", result)
	return result, nil
}

func main() {
	lambda.Start(handler)
}

// helper
func CalculateGPAAverage(r io.Reader) (float64, error) {
	reader := csv.NewReader(r)

	header, err := reader.Read()
	if err != nil {
		return 0, fmt.Errorf("failed to read header: %v", err)
	}

	if header[1] != "GPA" {
		return 0, fmt.Errorf("CSV file must have GPA as a second column")
	}

	var totalGPA float64
	var count int

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to read row: %v", err)
		}

		gpa, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			return 0, fmt.Errorf("invalid GPA value %q: %v", record[1], err)
		}

		totalGPA += gpa
		count++
	}

	if count == 0 {
		return 0, fmt.Errorf("no valid GPA data found")
	}

	return totalGPA / float64(count), nil
}
