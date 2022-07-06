import { Outlet, Route } from "react-router-dom";
import { Typography, Container } from "@mui/material";
import ProtectedRoute from "../auth/ProtectedRoute";


function getAdminRoute() {
  return (
    <Route path="/admin" element={<ProtectedRoute component={PipelinesBody} />}>
        <Route path="" element={<ProtectedRoute component={PipelinesListOutlet} />} />
    </Route>
  );
}

function PipelinesBody() {
    return (
        <Container>
            <Outlet />
        </Container>
    );
}

function PipelinesListOutlet() {
    return (
        <Container>
            <Typography>Hello World</Typography>
        </Container>
    );
}

export default getAdminRoute;
