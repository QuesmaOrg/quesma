package end2end

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type httpRequestParser struct{}

const testCasesDir = "testcases/"

func (p *httpRequestParser) getSingleTest(testSuite, testNr string) (*singleE2ETest, error) {
	file, err := os.Open(testCasesDir + testSuite + "/" + testNr + ".http")
	if err != nil {
		return nil, err
	}
	fmt.Println(file.Name(), err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return nil, scanner.Err()
	}

	urlSuffix := scanner.Text()
	bodyBuilder := strings.Builder{}
	for scanner.Scan() {
		bodyBuilder.WriteString(scanner.Text())
	}
	return &singleE2ETest{urlSuffix: urlSuffix, requestBody: bodyBuilder.String(), name: testNr}, nil
}

func (p *httpRequestParser) getSingleTestSuite(testSuite string) (tests []*singleE2ETest, err error) {
	var files []os.DirEntry
	files, err = os.ReadDir(testCasesDir + testSuite)
	if err != nil {
		return
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".http") {
			continue
		}
		testNr := strings.TrimSuffix(file.Name(), ".http")
		test, err := p.getSingleTest(testSuite, testNr)
		if err == nil {
			tests = append(tests, test)
		}
	}
	return
}

// getAllTestSuites returns all test suites in the testcases directory.
// So if in testcases/ there are directories 1, 3, and files 2.http, 4.http, it'll return ["1", "3"]
func (p *httpRequestParser) getAllTestSuites() (suiteNames []string, err error) {
	var files []os.DirEntry
	files, err = os.ReadDir(testCasesDir)
	if err != nil {
		return
	}
	for _, file := range files {
		if file.Type().IsDir() {
			suiteNames = append(suiteNames, file.Name())
		}
	}
	return
}
