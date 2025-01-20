package main

import (
	"context"
	"log"
	"math"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var (
	svc *dynamodb.Client
)

func init() {
	// Initialize the S3 client outside of the handler, during the init phase
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Create a new DynamoDB client
	svc = dynamodb.NewFromConfig(cfg)
}

type SalaryStats struct {
	MinSalary float64 `json:"minSalary"`
	MaxSalary float64 `json:"maxSalary"`
}

type User struct {
	Id       int     `dynamodbav:"title"`
	Name     string  `dynamodbav:"name"`
	Position string  `dynamodbav:"position"`
	Salary   float64 `dynamodbav:"salary"`
	Surname  string  `dynamodbav:"surname"`
}

func HandleRequest(ctx context.Context) (SalaryStats, error) {
	// Scan the table to get all items
	result, err := svc.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName: aws.String("Users"),
	})
	if err != nil {
		log.Fatalf("failed to scan table, %v", err)
		return SalaryStats{}, err
	}

	var users []User
	err = attributevalue.UnmarshalListOfMaps(result.Items, &users)
	if err != nil {
		log.Printf("Error unmarshaling users: %v", err)
		return SalaryStats{}, err
	}

	var minSalary, maxSalary float64
	minSalary = math.MaxFloat64
	maxSalary = -1 * math.MaxFloat64

	// Iterate over each item in the scan result
	for _, user := range users {
		if user.Salary < minSalary {
			minSalary = user.Salary
		}
		if user.Salary > maxSalary {
			maxSalary = user.Salary
		}
	}

	// Return the min and max salary as the result
	stats := SalaryStats{
		MinSalary: minSalary,
		MaxSalary: maxSalary,
	}

	return stats, nil
}

func main() {
	lambda.Start(HandleRequest)
}
