package end2end

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type httpRequestParser struct{}

const dir = "testcases/"

func (p *httpRequestParser) getSingleTest(testSuite, testNr string) (*singleE2ETest, error) {
	file, err := os.Open(dir + testSuite + "/" + testNr + ".http")
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

func (p *httpRequestParser) getSingleSuite(testSuite string) (tests []*singleE2ETest, err error) {
	var files []os.DirEntry
	files, err = os.ReadDir(dir + testSuite)
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

func (p *httpRequestParser) getAllTestcases() {

}
