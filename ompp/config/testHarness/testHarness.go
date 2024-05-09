package main

import (
    "fmt"
    "os"
    "github.com/openmpp/go/ompp/config"
)

const inputFile string = "../testdata/test.ompp.config.ini"
const outputFile string = "../testdata/test.output"

func main() {
    // Open test file and stream contents into string.
    // input, err := os.ReadFile(inputFile)
    // if err != nil {
    //     fmt.Println(err.Error())
    //     os.Exit(1)
    // }

    // Call JoinMultiLineValues on the string.
    //output := config.JoinMultiLineValues(string(input))

    // Call NewIni on test input file and output key value map to output file.
    output, err := config.NewIni(inputFile, "")
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }

    // Convert map to string:
    s := ""
    for k, v := range output {
        s += k
        s += " | "
        s += v
        s += "\n"
    }

    // Write ouput to file.
    err = os.WriteFile(outputFile, []byte(s), 0644)
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }

    os.Exit(0)
}
