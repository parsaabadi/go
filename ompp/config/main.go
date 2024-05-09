// Since I'm defining this to be part of the config package
// I can reference the other public members without imports.
// Does this need to be main package?
package config

import (
    "fmt"
    "os"
)

const inputFile string = "testdata/test.ompp.config.ini"

// Call JoinMultiLineValues on input from a test file 
// and print output to another file to compare changes.
func main() {
    // Open test file and stream contents into string variable.
    input, err := os.ReadFile(inputFile)
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }

    // Call JoinMultiLineValues on the string.
    output := JoinMultiLineValues(string(input))

    // Write ouput to file. To console for now.
    fmt.Println(output)
    os.Exit(0)
}
