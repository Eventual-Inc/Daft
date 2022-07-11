const baseUrl = process.env.NODE_ENV == "development" ? "https://localhost:8443" : "https://app.eventualcomputing.com"

export default function getConfig() {
  return {
    baseApiUrl: `${baseUrl}/api`,
    baseHubUrl: baseUrl,
    auth0Audience: "https://auth.eventualcomputing.com",
  };
}
