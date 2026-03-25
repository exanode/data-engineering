# 283. Move Zeroes

Given an integer array nums, move all 0's to the end of it while maintaining the relative order of the non-zero elements.

Note that you must do this in-place without making a copy of the array.

 

Example 1:
Input: nums = [0,1,0,3,12]
Output: [1,3,12,0,0]

Example 2:
Input: nums = [0]
Output: [0]
 

Constraints:
1 <= nums.length <= 104, 
-231 <= nums[i] <= 231 - 1

2 possibilities - first element zero, or non-zero.

## 1 - Zero @ Zero
Keep iterating till we find a non-zero element and keeping the swap anchor at 0, once we do, swap it with the zero at the zeroth index. Once swapped, we know that the next nonzero element will go to the 0+1 = 1st index, so we increase the swap anchor's value by 1. 

## 2 - Zero @ Non-Zero 
Keep iterating and keeping track of the swap anchor at the current index + 1, once it finds a zero, the swap anchor will stay there until it finds a non-zero element. Once it finds a non-zero element, it will swap it out and increase the swap anchor by 1. 

Finally, we just need to combine this logic in one cohesive loop. 

```python
class Solution:
    def moveZeroes(self, nums: List[int]) -> None:
        swapAnchor = 0
        for i in range(len(nums)):
            if nums[i] != 0:
                if i > swapAnchor:  # avoid useless swap
                    nums[swapAnchor], nums[i] = nums[i], nums[swapAnchor]
                swapAnchor += 1
```

