package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)


func reduceWordCount(files []string) map[string]int {
	counts := make(map[string]int)
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			fmt.Println("Error opening file:", file, err)
			continue
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, " ")
			if len(parts) != 2 {
				fmt.Println("Invalid line format in word count file:", line)
				continue
			}
			word := parts[0]
			count, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Println("Invalid count in word count file:", parts[1], err)
				continue
			}
			counts[word] += count
		}

		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading file:", file, err)
		}
	}
	return counts
}

func reduceInvertedIndex(files []string) map[string][]string {
	index := make(map[string]map[string]struct{})
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			fmt.Println("Error opening file:", file, err)
			continue
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, " ")
			if len(parts) != 2 {
				fmt.Println("Invalid line format in inverted index file:", line)
				continue
			}
			word := parts[0]
			filename := parts[1]

			if _, ok := index[word]; !ok {
				index[word] = make(map[string]struct{})
			}
			index[word][filename] = struct{}{}
		}

		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading file:", file, err)
		}
	}

	result := make(map[string][]string)
	for word, filesSet := range index {
		filesList := make([]string, 0, len(filesSet))
		for file := range filesSet {
			filesList = append(filesList, file)
		}
		sort.Strings(filesList)
		result[word] = filesList
	}
	return result
}

func writeWordCountOutput(outputPath string, counts map[string]int) {
	file, err := os.Create(outputPath)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		os.Exit(1)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	var keys []string
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		fmt.Fprintf(writer, "%s %d\n", key, counts[key])
	}
}

func writeInvertedIndexOutput(outputPath string, index map[string][]string) {
	file, err := os.Create(outputPath)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		os.Exit(1)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	var keys []string
	for k := range index {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		files := index[key]
		fmt.Fprintf(writer, "%s %s\n", key, strings.Join(files, ","))
	}
}

func ProcessReducer(reducerIndex int, task string) {
	pattern := fmt.Sprintf("../mapper_intermediate/reducer_%d_mapper_*.txt", reducerIndex)
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Println("Error finding reducer files:", err)
		os.Exit(1)
	}

	if len(files) == 0 {
		fmt.Printf("No intermediate files found for reducer %d\n", reducerIndex)
		return
	}

	outputPath := fmt.Sprintf("../reducer_intermediate/reducer_%d.txt", reducerIndex)

	switch task {
	case "wc":
		counts := reduceWordCount(files)
		writeWordCountOutput(outputPath, counts)
	case "ii":
		index := reduceInvertedIndex(files)
		writeInvertedIndexOutput(outputPath, index)
	default:
		fmt.Println("Invalid task for reducer:", task)
		os.Exit(1)
	}
}

func callReducer (index string, task string) {
	if _, err := os.Stat("../reducer_intermediate"); os.IsNotExist(err) {
		if err := os.Mkdir("../reducer_intermediate", 0755); err != nil {
			fmt.Println("Error creating directory:", err)
			return
		}
	}

	reducerIndex, err := strconv.Atoi(index)
	if err != nil {
		fmt.Println("Invalid reducer index:", err)
		os.Exit(1)
	}

	ProcessReducer(reducerIndex, task)
}