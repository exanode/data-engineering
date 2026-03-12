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
