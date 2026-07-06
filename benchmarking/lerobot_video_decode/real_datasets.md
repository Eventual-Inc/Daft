# Validation on public datasets

The batched decode was validated against the original per-row decode on six
public LeRobot v3 datasets from Hugging Face, chosen to cover the codec, fps,
resolution, and camera-count diversity found in the wild. Remote reads over
`hf://`, single machine; measured 2026-07-01.

## Method

[`real_datasets.py`](real_datasets.py) decodes the first 16 frames of episode 0
(every camera column) in a fresh process, timing the full
`lerobot.read(..., load_video_frames=True)` pipeline.
[`run_real_datasets.sh`](run_real_datasets.sh) runs it once per reader revision -
`daft/datasets/lerobot.py` at the PR's merge-base vs this branch - and checks
that the decoded frames are identical between the two.

## Results: 16 frames, all cameras

![original vs batched on public datasets](charts/chart_real_datasets.png)

| dataset | codec | resolution | fps | cams | original | batched | output |
| --- | --- | --- | --- | --- | --- | --- | --- |
| AlexFeng1/fa_putPlace_35 | av1 | 640x480 | 30 | 3 | 330s | 25.5s | identical |
| Cache-SCA/IsaacLab-SO101-...-push_button | h264 | 640x480 | 10 | 2 | 126s | 11.5s | identical |
| Helloworldali/pick-cube-maniskill | mp4v | 224x224 | 30 | 2 | 98s | 10.0s | identical |
| HCIS-Lab/soarm101-feeding-nuts | av1 | 1280x720 | 30 | 2 | 213s | 18.6s | identical |
| Jackie1/bridge_data_v2_convert | av1 | 256x256 | 5 | 1 | 132s | 33.5s | identical |
| DAVIAN-Robotics/robocasa-MG_3000 | av1 | 128x128 | 20 | 3 | 366s | 35.9s | identical |

Every frame pixel-identical across all six datasets; 4-13x faster.

## Results: scaling up (pepijn223/egodex-test, av1 1920x1080 @30)

![original vs batched at scale](charts/chart_scaling.png)

| run | frames | wall | per frame |
| --- | --- | --- | --- |
| original | 100 | 311.8s | 3.12s |
| batched | 100 | 25.6s | 0.26s |
| original | 632 (full dataset) | 1750.7s | 2.77s |
| batched | 632 (full dataset) | 115.8s | 0.18s |

Outputs identical between readers at both scales, including every frame of the
full 632-frame dataset. Batched cost grows with batches rather than frames
(each 16-row batch opens its shard once) - ~0.2s/frame vs ~3s/frame - so the
full dataset drops from 29 minutes to under 2.
