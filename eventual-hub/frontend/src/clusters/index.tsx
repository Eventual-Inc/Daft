import { Typography, Container } from "@mui/material";
import { Route, Outlet } from "react-router-dom";
import ProtectedRoute from "../auth/ProtectedRoute";


function getClustersRoute() {
  return (
    <Route path="/clusters" element={<ProtectedRoute component={FunctionsBody} />}>
        <Route path="" element={<ProtectedRoute component={FunctionsListOutlet} />} />
    </Route>
  );
}

function FunctionsBody() {
    return <Container maxWidth="xl"><Outlet /></Container>;
}

function FunctionsListOutlet() {
    return (
    <Container>
        <Typography>Hello World</Typography>
    </Container>
    );
}

export default getClustersRoute;
