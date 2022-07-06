import { createTheme } from "@mui/material/styles";

export const colors = ["#001219", "#0a9396", "#e9d8a6", "#ca6702", "#ae2012"];

export const theme = createTheme({
  palette: {
    primary: {
      main: "#660099",
    },
    secondary: {
      main: "#C4367A",
    },
    background: {
      default: "#f7f7f8",
      paper: "#fcfcfc",
    },
    success: {
      main: "#8AC926",
    },
    error: {
      main: "#F35B04",
    },
    warning: {
      main: "#FFCA3A",
    },
    info: {
      main: "#c247ff",
    }
  },
  typography: {
    font: "Roboto",
    color: "#160e1b",
    h1: {
      fontSize: 36,
      font: "Roboto",
      fontWeight: 800,
    },
    h2: {
      fontSize: 36,
      font: "Roboto",
      fontWeight: 600,
    },
    h3: {
      fontSize: 16,
      font: "Roboto",
      fontWeight: 600,
    },
    h4: {
      fontSize: 16,
      font: "Roboto",
      fontWeight: 400,
    },
    h5: {
      fontSize: 16,
      font: "Roboto",
      fontWeight: 400,
    },
    h6: {
      fontSize: 14,
      font: "Roboto",
      fontWeight: 600,
      textTransform: "uppercase",
      color: "#6f6076",
    },
    body1: {
      fontWeight: 500,
      color: "#160e1b",
    },
    subtitle1: {
      fontWeight: 400,
      color: "#6f6076",
    },
    subtitle2: {
      fontWeight: 300,
      color: "#6f6076",
    },
  }
});
