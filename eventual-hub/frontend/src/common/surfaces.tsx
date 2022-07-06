import { Typography, Paper, Stack, FormControl, InputLabel, Select, MenuItem } from "@mui/material";
import { styled } from '@mui/material/styles';

export const DashboardItem = styled(Paper)(({ theme }) => ({
    backgroundColor: theme.palette.background.paper,
    ...theme.typography.body1,
    padding: theme.spacing(2),
    textAlign: 'left',
    height: "100%",
  }));

export const GraphFilter = () => {
    return (
        <Stack direction="column" alignItems="flex-start" padding={1} spacing={2}>
            <Typography variant="body1">Show me </Typography>
            <FormControl variant="standard">
                <InputLabel id="status-label">Statistic</InputLabel>
                <Select
                    labelId="select-status"
                    id="select-status"
                    value={2}
                    label="Hide"
                >
                    <MenuItem value={0}>Num Triggered</MenuItem>
                    <MenuItem value={1}>Error Rates</MenuItem>
                    <MenuItem value={2}>Latency</MenuItem>
                    <MenuItem value={3}>Error Rates</MenuItem>
                </Select>
            </FormControl>
            <Typography variant="body1">For the past</Typography>
            <FormControl variant="standard">
                <InputLabel id="select-time-label">Past</InputLabel>
                <Select
                    labelId="select-time"
                    id="select-time"
                    value={3}
                    label="Show"
                >
                    <MenuItem value={3}>3 Hours</MenuItem>
                    <MenuItem value={6}>6 Hours</MenuItem>
                    <MenuItem value={12}>12 Hours</MenuItem>
                    <MenuItem value={24}>24 Hours</MenuItem>
                </Select>
            </FormControl>
        </Stack>
    );
};