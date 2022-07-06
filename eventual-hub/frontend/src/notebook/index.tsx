import { Container, Typography } from "@mui/material";
import { Route, Link as RouterLink } from "react-router-dom";
import ProtectedRoute from '../auth/ProtectedRoute';

function getNotebookRoute() {
  return (
    <Route path="/notebooks" element={<ProtectedRoute component={DashboardBody} />} />
  );
}

function DashboardBody() {
    return (
        <Container>
            <Typography>Hello World</Typography>
        </Container>
    );
}

export default getNotebookRoute;
