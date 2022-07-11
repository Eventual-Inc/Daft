const baseApiUrl = process.env.NODE_ENV == "development" ? "https://localhost:8443" : "https://api.eventualcomputing.com"
const baseUrl = process.env.NODE_ENV == "development" ? "https://localhost:3000" : "https://app.eventualcomputing.com"


export default function getConfig() {
  return {
    baseUrl: baseUrl,
    baseApiUrl: `${baseApiUrl}/api`,
    baseHubUrl: baseApiUrl,
    auth0Audience: "https://auth.eventualcomputing.com",
  };
}
