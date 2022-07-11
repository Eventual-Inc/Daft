import {
  BrowserRouter as Router,
  Routes,
  Route,
  Link as RouterLink,
  useLocation,
  useNavigate,
} from "react-router-dom";
import { Auth0Provider, useAuth0 } from "@auth0/auth0-react";
import { QueryClient, QueryClientProvider } from "react-query";
import {
  Stack,
  Box,
  Tabs,
  Tab,
  Typography,
  Button,
  CircularProgress,
} from "@mui/material";
import Container from '@mui/material/Container';
import MenuBookIcon from '@mui/icons-material/MenuBook';
import GridViewIcon from '@mui/icons-material/GridView';
import ManageAccountsIcon from '@mui/icons-material/ManageAccounts';

import { ReactQueryDevtools } from "react-query/devtools";
import logo from "./assets/logo.png"

import getNotebookRoute from "./notebook";
import getAdminRoute from "./admin";
import getClustersRoute from "./clusters";
import { theme } from "./theme";
import { useEffect } from "react";
import getConfig from "./config";

function TabbedMenu() {
  const navigate = useNavigate();
  const baseTabPaths = ["/notebooks", "/clusters", "/admin"];
  const currentLocation = useLocation().pathname;
  const matchTabIndex = currentLocation == "/" ? null : baseTabPaths
    .map((path) => currentLocation.startsWith(path))
    .findIndex((matched) => matched);

  return (
    <Stack direction="column" paddingTop={3} spacing={2} alignItems="center" sx={{
      background: `linear-gradient(to right bottom, ${theme.palette.primary.dark}, ${theme.palette.primary.light})`,
      color: "#fcf5ff",
    }}>
      <Box onClick={() => navigate("/", {replace: true})}>
        <img src={logo} alt="logo" width="120px"></img>
      </Box>
      <Tabs textColor="inherit" indicatorColor="secondary" orientation={"vertical"} value={matchTabIndex}>
        <Tab label={<Typography variant="h4">Notebook</Typography>} iconPosition="start" icon={<MenuBookIcon fontSize="small" />} component={RouterLink} to={baseTabPaths[0]} sx={{justifyContent: "flex-start"}}/>
        <Tab label={<Typography variant="h4">Clusters</Typography>} iconPosition="start" icon={<GridViewIcon fontSize="small" />} component={RouterLink} to={baseTabPaths[1]} sx={{justifyContent: "flex-start"}}/>
        <Tab label={<Typography variant="h4">Admin</Typography>} iconPosition="start" icon={<ManageAccountsIcon fontSize="small" />} component={RouterLink} to={baseTabPaths[2]} sx={{justifyContent: "flex-start"}}/>
      </Tabs>
    </Stack>
  );
}

function LoginPage() {
  const { user, isAuthenticated, logout, isLoading, loginWithRedirect } = useAuth0();
  var contents;
  if (isLoading) {
    contents = <CircularProgress />;
  } else if (isAuthenticated) {
    contents = <Stack padding={12} direction="column" alignItems="center">
      <Typography>Welcome, {user?.name}</Typography>
      <Button variant="contained" onClick={() => {logout({returnTo: getConfig().baseUrl});}}>Logout</Button>
    </Stack>;
  } else {
    contents = <Stack padding={12} direction="column" alignItems="center">
      <img width="50%" src="https://doodleipsum.com/700/outline?i=2c350be916b8b173cd3026cbcdea1acb" />
      <Button variant="contained" onClick={() => loginWithRedirect()}>Login</Button>
    </Stack>;
  }
  return <Container sx={{height: "100%", width: "100%"}}>
    {contents}
  </Container>;
}

/**
 * This is the global definition of all the routes in the application
 * Each route corresponds to one Component. Nested routes are inserted into
 * the <Outlet /> component in their immediate parent's `element`.
 *
 * @returns Routes with element for each route
 */
function RoutedBody() {
  return (
    <Routes>
      <Route path="/" element={<LoginPage />}></Route>
      {getNotebookRoute()}
      {getAdminRoute()}
      {getClustersRoute()}
    </Routes>
  );
}

function Root() {
  const queryClient = new QueryClient();
  return (
    <QueryClientProvider client={queryClient}>
      <ReactQueryDevtools initialIsOpen={false} />
      <Auth0Provider
        domain="eventual-dev.us.auth0.com"
        clientId="zByGOxzmeNJhfToH8ENnibcRbKzglexp"
        redirectUri={window.location.origin}
      >
        <Router>
          <Box
            sx={{ minHeight: "100vh", display: "flex", flexDirection: "row" }}
          >
            <TabbedMenu />
            <RoutedBody />
          </Box>
        </Router>
      </Auth0Provider>
    </QueryClientProvider>
  );
}

export default Root;
