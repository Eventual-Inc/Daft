import { useAuth0 } from "@auth0/auth0-react";
import getConfig from "../config";

const audience = getConfig().auth0Audience;

const useTokenGenerator = () => {
    const { getAccessTokenSilently, getAccessTokenWithPopup } = useAuth0();
    return () => {
        return getAccessTokenSilently({audience: audience}).catch(
            () => getAccessTokenWithPopup({audience: audience})
        );
    }
}

export default useTokenGenerator;
