import React from "react";

import { withAuthenticationRequired, useAuth0 } from '@auth0/auth0-react';
import { Stack, CircularProgress, Typography } from "@mui/material";

interface ProtectedRouteProps {
    component: React.ComponentType;
}

const ProtectedRoute = ({ component, ...args }: ProtectedRouteProps) => {
    const { user, isAuthenticated } = useAuth0();
    const Component = withAuthenticationRequired(component, {
        onRedirecting: () => <Stack padding={12} alignItems="center" sx={{width: "100%", height: "100%"}}><CircularProgress /></Stack>,
    });

    if (!isAuthenticated) {
        return <Component {...args} />;
    }

    const whitelist = ['eventualcomputing.com']; //authorized domains
    const userHasAccess = whitelist.some((domain) => {
        if (user === undefined || user.email === undefined) {
            return false;
        }
        const emailSplit = user!.email!.split('@');
        return emailSplit[emailSplit.length - 1].toLowerCase() === domain;
    });

    if (userHasAccess) {
        return <Component {...args} />;
    }
    return <Stack padding={2} alignItems="center" sx={{width: "100%", height: "100%"}}>
        <Typography>Unauthorized user: {user?.email}</Typography>
    </Stack>
};

export default ProtectedRoute;
