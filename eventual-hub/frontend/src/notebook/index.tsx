import { Button, CircularProgress, Container, Stack, Typography } from "@mui/material";
import { Route } from "react-router-dom";
import ProtectedRoute from '../auth/ProtectedRoute';
import {useQueryClient, useMutation, useQuery} from 'react-query'
import axios from "axios";
import getConfig from "../config";
import useTokenGenerator from "../auth/auth0";

function getNotebookRoute() {
  return (
    <Route path="/notebooks" element={<ProtectedRoute component={NotebookBody} />} />
  );
}

const launchNotebookServer = (getToken: () => Promise<string>) => async (data: {}) => {
  const token = await getToken();
  const { data: response } = await axios.post(
    getConfig().baseApiUrl + "/notebooks",
    {},
    {headers: {'content-type': 'application/json', 'Authorization': `Bearer ${token}`}},
  );
  return response.data;
};

const getNotebookServer = (getToken: () => Promise<string>) => async () => {
  const token = await getToken();
  const { data } = await axios.get(
    getConfig().baseApiUrl + "/notebooks",
    {headers: {'content-type': 'application/json', 'Authorization': `Bearer ${token}`}},
  );
  return data.server;
};

function NotebookBody() {
    const tokenGenerator = useTokenGenerator();
    const queryClient = useQueryClient()
    const { mutate, isLoading: launchServerIsLoading } = useMutation(
      launchNotebookServer(tokenGenerator),
      {
        onSuccess: data => {
          console.log(data);
        },
        onError: () => {
          alert("there was an error")
        },
        onSettled: () => {
          queryClient.invalidateQueries('notebook_server');
        },
    });
    const { data, isLoading: getServerIsLoading } = useQuery(
      "notebook_server",
      getNotebookServer(tokenGenerator),
    );

    var button = <CircularProgress />;
    if (!getServerIsLoading && data !== null) {
      button = <Button onClick={() => { window.open(getConfig().baseHubUrl + data); } }>Connect to Notebook</Button>
    } else if (!getServerIsLoading && data === null) {
      button = <Button onClick={mutate}>Launch Notebook</Button>;
    }

    return (
        <Container>
          <Stack direction="column" spacing={2} padding={4}>
            <Typography variant="h2">Notebooks</Typography>
            {button}
          </Stack>
        </Container>
    );
}

export default getNotebookRoute;
