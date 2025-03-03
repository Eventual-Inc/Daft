#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LLMParameterizedPromptFragment {
    Text(String),
    Column(String),
}

impl LLMParameterizedPromptFragment {
    pub fn new_from_str(s: &str) -> Vec<Self> {
        #[derive(PartialEq)]
        enum State {
            Text,
            Escape,
            Column,
        }

        struct PromptParser<I: Iterator<Item = char>> {
            iter: std::iter::Peekable<I>,
            state: State,
            current_text: String,
            current_column: String,
            fragments: Vec<LLMParameterizedPromptFragment>,
        }

        impl<I: Iterator<Item = char>> PromptParser<I> {
            fn new(iter: I) -> Self {
                Self {
                    iter: iter.peekable(),
                    state: State::Text,
                    current_text: String::new(),
                    current_column: String::new(),
                    fragments: Vec::new(),
                }
            }

            fn push_text(&mut self) {
                if !self.current_text.is_empty() {
                    self.fragments
                        .push(LLMParameterizedPromptFragment::Text(std::mem::take(
                            &mut self.current_text,
                        )));
                }
            }

            fn push_column(&mut self) {
                if !self.current_column.is_empty() {
                    self.fragments
                        .push(LLMParameterizedPromptFragment::Column(std::mem::take(
                            &mut self.current_column,
                        )));
                }
            }

            fn parse(mut self) -> Vec<LLMParameterizedPromptFragment> {
                while let Some(c) = self.iter.next() {
                    match self.state {
                        State::Text => match c {
                            '\\' => self.state = State::Escape,
                            '{' => {
                                self.push_text();
                                self.state = State::Column;
                            }
                            _ => self.current_text.push(c),
                        },
                        State::Escape => {
                            match c {
                                '{' | '}' => self.current_text.push(c),
                                _ => {
                                    self.current_text.push('\\');
                                    self.current_text.push(c);
                                }
                            }
                            self.state = State::Text;
                        }
                        State::Column => {
                            if c == '}' {
                                self.push_column();
                                self.state = State::Text;
                            } else {
                                self.current_column.push(c);
                            }
                        }
                    }
                }

                // Handle any remaining text
                self.push_text();
                self.fragments
            }
        }

        PromptParser::new(s.chars()).parse()
    }
}
