from __future__ import annotations

import time

import ray

ray.init()


# Simple distributed computation
@ray.remote
def compute_square(x):
    return x * x


print("Ray cluster nodes:", len(ray.nodes()))
print("Ray cluster resources:", ray.cluster_resources())

# Test parallel execution
start_time = time.time()
futures = [compute_square.remote(i) for i in range(100)]
results = ray.get(futures)
end_time = time.time()

expected_sum = sum(i * i for i in range(100))
actual_sum = sum(results)

print(f"Parallel computation completed in {end_time - start_time:.2f}s")
print(f"Expected sum: {expected_sum}, Actual sum: {actual_sum}")

if actual_sum == expected_sum:
    print("✓ Ray cluster test passed!")
else:
    print("❌ Ray computation failed!")
