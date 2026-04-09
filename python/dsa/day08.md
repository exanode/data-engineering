# 125. Valid Palindrome

A phrase is a palindrome if, after converting all uppercase letters into lowercase letters and removing all non-alphanumeric characters, it reads the same forward and backward. Alphanumeric characters include letters and numbers.

Given a string s, return true if it is a palindrome, or false otherwise.

Example 1:
Input: s = "A man, a plan, a canal: Panama"
Output: true
Explanation: "amanaplanacanalpanama" is a palindrome.


Example 2:
Input: s = "race a car"
Output: false
Explanation: "raceacar" is not a palindrome.


Example 3:
Input: s = " "
Output: true
Explanation: s is an empty string "" after removing non-alphanumeric characters.
Since an empty string reads the same forward and backward, it is a palindrome.


```python
# -----------------------------
# Verbose (ASCII-based approach)
# -----------------------------
class Solution:
    def isPalindrome(self, s: str) -> bool:
        # List to store only valid alphanumeric characters (lowercased)
        res = []
        
        # ASCII ranges for:
        # uppercase letters (A-Z), lowercase letters (a-z), digits (0-9)
        u_min, u_max = ord('A'), ord('Z')
        l_min, l_max = ord('a'), ord('z')
        d_min, d_max = ord('0'), ord('9')
        
        for ch in s:
            val = ord(ch)  # Convert character to ASCII value
            
            # Check if character is alphanumeric using ASCII ranges
            if (u_min <= val <= u_max) or \
               (l_min <= val <= l_max) or \
               (d_min <= val <= d_max):
                
                # Convert to lowercase and store
                res.append(ch.lower())
        
        # Check if filtered list is equal to its reverse
        return res == res[::-1]
```

```python
# -----------------------------
# Pythonic (clean + readable)
# -----------------------------
class Solution: 
    def isPalindrome(self, s: str) -> bool:
        # Keep only alphanumeric characters using built-in function
        # filter(str.isalnum, s) removes non-alphanumeric chars
        new_string = "".join(filter(str.isalnum, s)) 
        
        # Convert to lowercase and compare with reversed version
        return new_string.lower() == new_string[::-1].lower()
```

```python
# -----------------------------
# Optimal (Two-pointer approach)
# -----------------------------
class Solution:
    def isPalindrome(self, s: str) -> bool:
        # Initialize two pointers at start and end
        l, r = 0, len(s) - 1
        
        # Loop until pointers meet
        while l < r:
            
            # Move left pointer forward if not alphanumeric
            while l < r and not s[l].isalnum():
                l += 1
            
            # Move right pointer backward if not alphanumeric
            while l < r and not s[r].isalnum():
                r -= 1
            
            # Compare characters (case-insensitive)
            if s[l].lower() != s[r].lower():
                return False  # Mismatch → not a palindrome
            
            # Move both pointers inward
            l += 1
            r -= 1
        
        # If all characters matched
        return True
```