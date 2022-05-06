package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/manifoldco/promptui"
)

type selectPromptData struct {
	Name        string
	Value       string
	Description string
}

// Starts a prompt for textual responses
func TextPrompt(label string) (string, error) {
	prompt := promptui.Prompt{
		Label: label,
	}
	return prompt.Run()
}

// Starts a prompt for boolean responses
func BoolPrompt(label string) (bool, error) {
	prompt := promptui.Prompt{
		Label: fmt.Sprintf("%s [y/N]", label),
	}
	result, err := prompt.Run()
	if err != nil {
		return false, err
	}
	if strings.ToLower(result) == "y" {
		return true, nil
	} else if strings.ToLower(result) == "n" {
		return false, nil
	}
	return false, fmt.Errorf("received unexpected input (expected y or N): %s", result)
}

// Starts the default editor for users to interactively edit a template string
func EditorPrompt(template string, fileext string) (string, error) {
	dir, err := ioutil.TempDir("", "daftcli-*")
	defer os.RemoveAll(dir)
	if err != nil {
		return "", err
	}

	f, err := os.CreateTemp(dir, fmt.Sprintf("*.%s", fileext))
	defer os.Remove(f.Name())
	if err != nil {
		return "", err
	}
	f.Write([]byte(template))

	cmd := exec.Command("vim", f.Name())
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	err = cmd.Run()
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(f.Name())
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// Starts a prompt for selecting a single choice from a list of items
func SelectPrompt(label string, help string, items []selectPromptData) (selectPromptData, error) {
	templates := &promptui.SelectTemplates{
		Label:    fmt.Sprintf(`{{ "%s" | faint }}`, help),
		Active:   "> {{ .Name }} ({{ .Value | red }})",
		Inactive: "  {{ .Name }} ({{ .Value | red }})",
		Selected: fmt.Sprintf(`{{ "%s: " | faint }} {{ .Name }}`, label),
		Details: `
	{{ .Description }}`,
		Help: fmt.Sprintf(`{{ "%s" | bold }}`, label),
	}
	prompt := promptui.Select{
		Label:     label,
		Items:     items,
		Templates: templates,
	}
	i, _, err := prompt.Run()
	if err != nil {
		return selectPromptData{}, err
	}
	return items[i], nil
}
