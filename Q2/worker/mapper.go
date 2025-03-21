package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func hash(s string, numReducers uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32() % numReducers
}

func processFile(filename string, processWord func(string)) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		processWord(strings.ToLower(scanner.Text()))
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", filename, err)
		os.Exit(1)
	}
}

func WordCountMapper(inputFilename, mapperIndex string, numReducers int) {
	wordCount := make(map[string]int)

	processFile(inputFilename, func(word string) {
		wordCount[word]++
	})

	writeToReducers(wordCount, inputFilename, mapperIndex, numReducers, false)
}

func InvertedIndexMapper(inputFilename, mapperIndex string, numReducers int) {
	wordSet := make(map[string]struct{})
	processFile(inputFilename, func(word string) {
		wordSet[word] = struct{}{}
	})

	wordCount := make(map[string]int)
	for word := range wordSet {
		wordCount[word] = 1
	}

	originalFilename := strings.SplitN(inputFilename, "_mapper_", 2)[0]
	writeToReducers(wordCount, originalFilename, mapperIndex, numReducers, true)
}

func writeToReducers(wordData map[string]int, filename, mapperIndex string, numReducers int, isInvertedIndex bool) {
	files := make(map[uint32]*os.File)
	writers := make(map[uint32]*bufio.Writer)

	originalFilename := strings.Split(filename, "_mapper_")[0] + ".txt"

	for word, count := range wordData {
		hashValue := hash(word, uint32(numReducers))
		filePath := fmt.Sprintf("../mapper_intermediate/reducer_%d_mapper_%s.txt", hashValue, mapperIndex)

		if _, exists := files[hashValue]; !exists {
			f, err := os.Create(filePath)
			if err != nil {
				fmt.Println("Error creating file:", err)
				os.Exit(1)
			}
			files[hashValue] = f
			writers[hashValue] = bufio.NewWriter(f)
		}

		if isInvertedIndex {
			fmt.Fprintf(writers[hashValue], "%s %s\n", word, originalFilename)
		} else {
			fmt.Fprintf(writers[hashValue], "%s %d\n", word, count)
		}
	}

	for _, writer := range writers {
		writer.Flush()
	}
	for _, f := range files {
		f.Close()
	}
}

func FindFileByMapperIndex(mapperIndex string, directory string) (string, error) {
	directory = filepath.Join("..", directory)
	files, err := filepath.Glob(filepath.Join(directory, "*_mapper_*.txt"))
	if err != nil {
		return "", err
	}

	rgx := regexp.MustCompile(`.*_mapper_(\d+)\.txt$`)
	matchingFiles := []string{}

	for _, file := range files {
		matches := rgx.FindStringSubmatch(file)
		if len(matches) > 1 && matches[1] == mapperIndex {
			matchingFiles = append(matchingFiles, file)
		}
	}

	if len(matchingFiles) == 0 {
		return "", fmt.Errorf("no file found for mapper index %s", mapperIndex)
	}

	sort.Strings(matchingFiles)
	return matchingFiles[0], nil
}

func callMapper (index string, task string, numReducers int) {
	if _, err := os.Stat("../mapper_intermediate"); os.IsNotExist(err) {
		if err := os.Mkdir("../mapper_intermediate", 0755); err != nil {
			fmt.Println("Error creating directory:", err)
			return
		}
	}

	inputFilename, err := FindFileByMapperIndex(index, "file_chunks")
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	switch task {
	case "wc":
		WordCountMapper(inputFilename, index, numReducers)
	case "ii":
		InvertedIndexMapper(inputFilename, index, numReducers)
	default:
		fmt.Println("Invalid task. Use 'wc' for word count or 'ii' for inverted index.")
		os.Exit(1)
	}
}