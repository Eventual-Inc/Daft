package cmd

import (
	"fmt"

	"github.com/manifoldco/promptui"
)

type selectPromptData struct {
	Name        string
	Value       string
	Description string
}

func SelectPrompt(label string, help string, items []*selectPromptData) (*selectPromptData, error) {
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
		return nil, err
	}
	return items[i], nil
}
