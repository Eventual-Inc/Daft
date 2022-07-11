import { Button, CircularProgress, Container, Stack, Typography } from "@mui/material";
import { Route } from "react-router-dom";
import ProtectedRoute from '../auth/ProtectedRoute';
import {useQueryClient, useMutation, useQuery} from 'react-query'
import axios from "axios";
import getConfig from "../config";
import useTokenGenerator from "../auth/auth0";
import { useState } from "react";

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

const stopNotebookServer = (getToken: () => Promise<string>) => async (data: {}) => {
  const token = await getToken();
  const { data: response } = await axios.delete(
    getConfig().baseApiUrl + "/notebooks",
    {headers: {'content-type': 'application/json', 'Authorization': `Bearer ${token}`}},
  );
  return response.data;
};

interface UserNotebookDetails {
  url?: string
  ready: boolean
  pending?: string
}

const getNotebookServer = (getToken: () => Promise<string>) => async () => {
  const token = await getToken();
  const { data } = await axios.get<UserNotebookDetails>(
    getConfig().baseApiUrl + "/notebooks",
    {headers: {'content-type': 'application/json', 'Authorization': `Bearer ${token}`}},
  );
  return data;
};

function NotebookBody() {
    const tokenGenerator = useTokenGenerator();
    const queryClient = useQueryClient()
    const { mutate: mutateLaunchNotebook, isLoading: launchServerIsLoading } = useMutation(
      launchNotebookServer(tokenGenerator),
      {
        onSettled: () => {
          queryClient.invalidateQueries('notebook_server');
        },
    });
    const { mutate: mutateStopNotebook, isLoading: stopServerIsLoading } = useMutation(
      stopNotebookServer(tokenGenerator),
      {
        onSettled: () => {
          queryClient.invalidateQueries('notebook_server');
        },
    });
    const { data, isLoading: getServerIsLoading } = useQuery(
      "notebook_server",
      getNotebookServer(tokenGenerator),
    );

    var button = <CircularProgress />;
    var deleteNotebookButton = <></>;
    if (!getServerIsLoading) {
      if (data === undefined) {
        button = <Typography>An error has occurred.</Typography>;
      } else if (!data.ready && data.pending === null) {
        button = <Button onClick={mutateLaunchNotebook} disabled={launchServerIsLoading}>Launch Notebook</Button>;
      } else if (data.url !== undefined && data.pending == "pending") {
        button = <Button disabled={true}>Notebook is starting...</Button>
        deleteNotebookButton = <Button onClick={mutateStopNotebook} disabled={stopServerIsLoading}>Delete Notebook</Button>;
      } else if (data.url !== undefined) {
        button = <Button onClick={() => { window.open(getConfig().baseHubUrl + data?.url); } }>Connect to Notebook</Button>;
        deleteNotebookButton = <Button onClick={mutateStopNotebook} disabled={stopServerIsLoading}>Delete Notebook</Button>;
      }
    }

    return (
        <Container>
          <Stack direction="column" spacing={2} padding={4}>
            <Typography variant="h2">Notebooks</Typography>
            {button}
            {deleteNotebookButton}
          </Stack>
        </Container>
    );
}

export default getNotebookRoute;
