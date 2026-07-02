# Validation on public datasets

The batched decode was validated against the original per-row decode on public
LeRobot v3 datasets from Hugging Face, chosen from a survey of LeRobot-tagged
datasets to cover the codec, fps, resolution, and camera-count diversity found
in the wild. Measured 2026-07-01, remote reads over `hf://`, single machine.

## Method

For each dataset, decode the first 16 frames of episode 0 (every camera column)
twice - once with the original per-row reader and once with the batched reader -
in fresh processes. Every decoded frame is hashed (SHA-256 of the raw RGB
pixels) and the two runs are compared hash-for-hash on identical rows. Wall
time covers the full `lerobot.read(..., load_video_frames=True)` pipeline.

## Results: 16 frames, all cameras

| dataset | codec | resolution | fps | cams | original | batched | output |
| --- | --- | --- | --- | --- | --- | --- | --- |
| AlexFeng1/fa_putPlace_35 | av1 | 640x480 | 30 | 3 | 330s | 25.5s | identical |
| Cache-SCA/IsaacLab-SO101-...-push_button | h264 | 640x480 | 10 | 2 | 126s | 11.5s | identical |
| Helloworldali/pick-cube-maniskill | mp4v | 224x224 | 30 | 2 | 98s | 10.0s | identical |
| HCIS-Lab/soarm101-feeding-nuts | av1 | 1280x720 | 30 | 2 | 213s | 18.6s | identical |
| Jackie1/bridge_data_v2_convert | av1 | 256x256 | 5 | 1 | 132s | 33.5s | identical |
| DAVIAN-Robotics/robocasa-MG_3000 | av1 | 128x128 | 20 | 3 | 366s | 35.9s | identical |

Every frame pixel-identical across all six datasets; 4-13x faster. The 5 fps
dataset exercises the tolerance logic at its loosest (tolerance is half a frame
period, so 0.1s there vs 0.017s at 30 fps). Multi-camera datasets hit the
per-frame cost hardest with the original reader, since every camera multiplies
the per-frame opens.

## Results: scaling up (pepijn223/egodex-test, av1 1920x1080 @30)

| run | frames | wall | per frame |
| --- | --- | --- | --- |
| original | 100 | 311.8s | 3.12s |
| batched | 100 | 25.6s | 0.26s |
| batched | 632 (full dataset) | 115.8s | 0.18s |

- The 100-frame outputs are hash-identical between readers.
- The full-dataset batched run was spot-checked by re-decoding 10 rows spread
  across the range with the original reader: 0 hash mismatches.
- At scale the batched cost grows with the number of batches rather than
  frames (each 16-row batch opens its shard once), so it is linear with a much
  smaller slope - ~0.2s/frame here vs ~3.1s/frame for the original.
