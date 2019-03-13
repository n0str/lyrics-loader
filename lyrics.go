package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Jeffail/tunny"
	"github.com/antchfx/htmlquery"
	"io/ioutil"
	"os"
	"sync"
)

type Song struct {
	Artist string
	Song string
	Link string
	Text string
}

type Result struct {
	Artist string
	Song string
	Tags []string
}

const MAXWORKERS = 80

func Populate(method func(payload interface{}) interface{}, workersNumber int, tasks []Song)  {
	if workersNumber > MAXWORKERS {
		workersNumber = MAXWORKERS
	}
	pool := tunny.NewFunc(workersNumber, method)
	defer pool.Close()

	var wg sync.WaitGroup
	wg.Add(len(tasks))

	for _, element := range tasks {
		go func(task Song) {
			pool.Process(task)
			wg.Done()
		}(element)
	}

	wg.Wait()
}

func ReadCsvFile(filePath string) []Song {
	songs := []Song{}

	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		panic(err)
	}

	for _, line := range lines {
		songs = append(songs, Song{
			Artist: line[0],
			Song: line[1],
			Link: line[2],
			Text: line[3],
		})
	}

	return songs
}

func main() {
	inputFile := flag.String("inputFile", "songdata.csv", "input CSV file")
	outputFile := flag.String("outputFile", "results.json", "output JSON file")
	songsFrom := flag.Int("songsFrom", 1, "number of the first song in CSV")
	songsTo := flag.Int("songsTo", 20, "number of the last song in CSV")
	workers := flag.Int("workers", 20, "number of concurrent workers")

	flag.Parse()

	allSongs := ReadCsvFile(*inputFile)

	var songsFromValue = *songsFrom
	var songsToValue = *songsTo

	fmt.Printf("Gonna load songs from %d to %d of %d songs\n", songsFromValue, songsToValue, len(allSongs))
	songs := allSongs[songsFromValue:songsToValue]

	ch := make(chan Result, len(songs))

	Populate(func(payload interface{}) interface{} {

		song := payload.(Song)
		url := fmt.Sprintf("https://www.last.fm/music/%s/_/%s", song.Artist, song.Song)

		doc, err := htmlquery.LoadURL(url)
		if err != nil {
			fmt.Printf("Error: %s: %s", url, err)
			return nil
		}

		tags := []string{}
		for _, n := range htmlquery.Find(doc, "//*[@id=\"mantle_skin\"]/div[4]/div/div[1]/section[1]/ul/li") {
			tags = append(tags, htmlquery.InnerText(n))
		}

		fmt.Printf("Finished: %s. Found %d tags.\n", url, len(tags))

		ch <- Result{Artist:song.Artist, Song:song.Song, Tags:tags}

		return nil
	}, *workers, songs)

	results := []Result{}
	channelSize := len(ch)
	for i := 0; i < channelSize; i++ {
		results = append(results, <-ch)
	}

	resultsJson, _ := json.Marshal(results)
	err := ioutil.WriteFile(*outputFile, resultsJson, 0644)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Done.")
}