442. Find All Duplicates in an Array

Given an integer array nums of length n where all the integers of nums are in the range [1, n] and each integer appears at most twice, return an array of all the integers that appears twice.

You must write an algorithm that runs in O(n) time and uses only constant auxiliary space, excluding the space needed to store the output

 

Example 1:
Input: nums = [4,3,2,7,8,2,3,1]
Output: [2,3]

Example 2:
Input: nums = [1,1,2]
Output: [1]

Example 3:
Input: nums = [1]
Output: []
 

Constraints:
n == nums.length
1 <= n <= 105
1 <= nums[i] <= n
Each element in nums appears once or twice.

The trick here is to identify that the values in the array have this constraint "1 <= nums[i] <= n" (positive and <= length of array). Also, this - "Each element in nums appears once or twice." And lastly, that the data structure here is an array that is mutable. 

What we do with this information? We use a method called index marking. 

Index marking - Here, since the array values cannot exceed the array length, we can use that info then to mark the value's index as negative (or anything else that stands out). 

We first check if the value's index is negative, if yes, then it was already processed and we copy that to the result.


```python
class Solution:
    def findDuplicates(self, nums: List[int]) -> List[int]:
        result = []
        # [4,3,2,7,8,2,3,1]
        for i in range(len(nums)):
            val_idx = abs(nums[i]) - 1
            if nums[val_idx] < 0:
                result.append(abs(nums[i]))
            nums[val_idx] = - nums[val_idx]
        return result
```