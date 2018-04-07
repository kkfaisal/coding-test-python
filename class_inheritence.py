"""
Answer for question number 8.
"""

class BaseClass(object):
    def __init__(self, atrib1,atrib2,atrib3):
        self.batrib1=atrib1
        self.batrib2=atrib2
        self.batrib3=atrib3

    def display(self,child):
        print(self.batrib1,self.batrib2,self.batrib3,child.catrib1,child.catrib2,child.catrib3)

class ChildClass(BaseClass):
    def __init__(self, base_atrib1,base_atrib2,base_atrib3,atrib1,atrib2,atrib3):
        self.catrib1=atrib1
        self.catrib2=atrib2
        self.catrib3=atrib3
        super().__init__(base_atrib1,base_atrib2,base_atrib3)

    def display(self):
        super().display(self)

cs =ChildClass(1,2,3,11,12,13)
cs.display()
