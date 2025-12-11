package main

import (
    "encoding/csv"
    "fmt"
    "log"
    "os"
    "strconv"

    "github.com/go-gota/gota/dataframe"
    "gonum.org/v1/plot"
    "gonum.org/v1/plot/plotter"
)

func readCSV(path string) dataframe.DataFrame {
    f, err := os.Open(path)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()
    return dataframe.ReadCSV(f)
}

func writeCSV(df dataframe.DataFrame, path string) {
    f, err := os.Create(path)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()
    w := csv.NewWriter(f)
    w.Write(df.Names())
    for i := 0; i < df.Nrow(); i++ {
        var row []string
        for _, col := range df.Names() {
            row = append(row, df.Col(col).Elem(i).String())
        }
        w.Write(row)
    }
    w.Flush()
}

func main() {
    students := readCSV("bronze/student.csv")
    grades := readCSV("bronze/grades.csv")

    // SILVER: clean & join
    students = students.Filter(students.Col("name").Ne("")).
        Filter(students.Col("age").Ne(""))
    grades = grades.Filter(grades.Col("grade").Ne(""))

    unified := students.
        InnerJoin(grades, "student_id")

    writeCSV(unified, "silver/unified.csv")

    // GOLD: star schema (dim_student + fact_grades)
    star := unified.Select([]string{"student_id", "name", "age", "course", "grade"})
    writeCSV(star, "gold/star.csv")

    // Simple scatter: age vs grade
    pts := make(plotter.XYs, star.Nrow())
    for i := 0; i < star.Nrow(); i++ {
        age, _ := strconv.ParseFloat(star.Col("age").Elem(i).String(), 64)
        grade, _ := strconv.ParseFloat(star.Col("grade").Elem(i).String(), 64)
        pts[i].X = age
        pts[i].Y = grade
    }

    p := plot.New()
    s, _ := plotter.NewScatter(pts)
    p.Add(s)
    p.X.Label.Text = "Age"
    p.Y.Label.Text = "Grade"
    p.Save(4*96, 4*96, "plots/scatter.png")

    fmt.Println("Pipeline finished. See silver/unified.csv, gold/star.csv, plots/scatter.png")
}
