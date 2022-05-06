package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/manifoldco/promptui"
)

type selectPromptData struct {
	Name        string
	Value       string
	Description string
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
