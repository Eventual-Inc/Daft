const baseApiUrl = process.env.NODE_ENV == "development" ? "https://dev-api.eventualcomputing.com" : process.env.NODE_ENV == "production" ? "https://api.eventualcomputing.com" : "https://localhost:8443"
const baseUrl = process.env.NODE_ENV == "development" ? "https://dev-app.eventualcomputing.com" : process.env.NODE_ENV == "production" ? "https://app.eventualcomputing.com" : "https://localhost:3000"
const auth0Domain = process.env.NODE_ENV == "production" ? "eventual-prod.us.auth0.com" : "eventual-dev.us.auth0.com"
const auth0ClientId = process.env.NODE_ENV == "production" ? "eventual-prod.us.auth0.com" : "zByGOxzmeNJhfToH8ENnibcRbKzglexp"

export default function getConfig() {
  return {
    baseUrl: baseUrl,
    baseApiUrl: `${baseApiUrl}/api`,
    baseHubUrl: baseApiUrl,
    auth0Audience: "https://auth.eventualcomputing.com",
    auth0Domain: auth0Domain,
    auth0ClientId: auth0ClientId,
  };
}
