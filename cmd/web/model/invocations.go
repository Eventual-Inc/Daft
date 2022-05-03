package model

type CreateInvocation struct {
	ID     string      `json:"id" example:"1" format:"int64"`
	Inputs interface{} `json:"inputs" example:"{}"`
}

type CreateInvocationSuccess struct {
	ID string `json:"id" example:"1" format:"int64"`
}
