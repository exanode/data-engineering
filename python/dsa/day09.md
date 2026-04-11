# 189. Rotate Array

Given an integer array nums, rotate the array to the right by k steps, where k is non-negative.

 

Example 1:
Input: nums = [1,2,3,4,5,6,7], k = 3
Output: [5,6,7,1,2,3,4]
Explanation:
rotate 1 steps to the right: [7,1,2,3,4,5,6]
rotate 2 steps to the right: [6,7,1,2,3,4,5]
rotate 3 steps to the right: [5,6,7,1,2,3,4]

Example 2:
Input: nums = [-1,-100,3,99], k = 2
Output: [3,99,-1,-100]
Explanation: 
rotate 1 steps to the right: [99,-1,-100,3]
rotate 2 steps to the right: [3,99,-1,-100]
 

Constraints:

1 <= nums.length <= 105
-231 <= nums[i] <= 231 - 1
0 <= k <= 105
 

```python
class Solution:
    def rotate(self, nums: List[int], k: int) -> None:
        # Get the length of the list
        len_nums = len(nums)

        # Reduce k to avoid unnecessary full rotations
        # Example: rotating 7 times in a list of size 5 is same as rotating 2 times
        k_factor = k if k < len_nums else k % len_nums

        # If:
        # - only one element (no change possible)
        # - k equals length (full rotation → same array)
        # - k is 0 (no rotation needed)
        # then return as is
        if len_nums == 1 or k_factor == len_nums or k_factor == 0:
            return nums        
        
        # Perform rotation:
        # Take last k elements → nums[-k_factor:]
        # Take remaining front elements → nums[:len_nums - k_factor]
        # Combine them to form rotated list
        nums[:] = nums[-k_factor:] + nums[:len_nums - k_factor]

        # Return rotated list (even though LeetCode expects in-place modification)
        return nums
```