package main

import (
    "fmt"
    "log"
    "os"

    "github.com/go-gota/gota/dataframe"
)

func main() {
    f, err := os.Open("data/example.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()

    df := dataframe.ReadCSV(f)

    pop := df.Col("population").Float()
    maxIndex := 0
    for i, v := range pop {
        if v > pop[maxIndex] {
            maxIndex = i
        }
    }

    fmt.Println("Largest city:", df.Col("city").Elem(maxIndex), "with", pop[maxIndex])

    df = df.Mutate(dataframe.LoadMaps(
        []map[string]interface{}{
            {"population_thousands": pop[0] / 1000},
            {"population_thousands": pop[1] / 1000},
            {"population_thousands": pop[2] / 1000},
            {"population_thousands": pop[3] / 1000},
        },
    ))

    fmt.Println("\nTransformed data:")
    fmt.Println(df)
}
