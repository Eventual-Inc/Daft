const baseApiUrl = process.env.REACT_APP_BUILD_ENVIRONMENT == "development" ? "https://dev-api.eventualcomputing.com" : process.env.REACT_APP_BUILD_ENVIRONMENT == "production" ? "https://api.eventualcomputing.com" : "https://localhost:8443"
const baseUrl = process.env.REACT_APP_BUILD_ENVIRONMENT == "development" ? "https://dev-app.eventualcomputing.com" : process.env.REACT_APP_BUILD_ENVIRONMENT == "production" ? "https://app.eventualcomputing.com" : "https://localhost:3000"
const auth0Domain = process.env.REACT_APP_BUILD_ENVIRONMENT == "production" ? "eventual-prod.us.auth0.com" : "eventual-dev.us.auth0.com"
const auth0ClientId = process.env.REACT_APP_BUILD_ENVIRONMENT == "production" ? "iwPBkx4iP1PJ4hML2C4eD2cUFYjT5vUH" : "zByGOxzmeNJhfToH8ENnibcRbKzglexp"

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
