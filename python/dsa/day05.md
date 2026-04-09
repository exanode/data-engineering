# 179. Largest Number
Given a list of non-negative integers nums, arrange them such that they form the largest number and return it.

Since the result may be very large, so you need to return a string instead of an integer.

 

Example 1:
Input: nums = [10,2]
Output: "210"

Example 2:
Input: nums = [3,30,34,5,9]
Output: "9534330"
 

Constraints:

1 <= nums.length <= 100
0 <= nums[i] <= 109

```python
class Solution:
    def largestNumber(self, nums: List[int]) -> str:
        num_strings = []

        # Convert integers to strings
        for num in nums:
            num_strings.append(str(num))

        length = len(num_strings)

        # Sort numbers based on concatenation comparison
        for i in range(length):
            for j in range(i + 1, length):
                first_combination = num_strings[i] + num_strings[j]
                second_combination = num_strings[j] + num_strings[i]

                if first_combination < second_combination:
                    num_strings[i], num_strings[j] = num_strings[j], num_strings[i]

        result = ''.join(num_strings)

        # Handle case like [0,0]
        if int(result) == 0:
            return "0"

        return result
```

```
class Solution:
    def largestNumber(self, nums):
        # Convert all integers to strings for concatenation comparison
        nums = list(map(str, nums))
        
        # Perform insertion sort based on custom comparison
        for i in range(1, len(nums)):
            to_left = i - 1
            anchor = nums[i]  # Current element to position correctly
            
            # Shift elements to the right if they form a smaller number
            # when placed before 'anchor'
            while to_left >= 0 and nums[to_left] + anchor < anchor + nums[to_left]:
                nums[to_left + 1] = nums[to_left]
                to_left -= 1
            
            # Place the anchor at its correct position
            nums[to_left + 1] = anchor
        
        # Edge case: if the largest number is '0', return '0'
        # (handles cases like [0, 0])
        return ''.join(nums) if nums[0] != '0' else '0'
```
