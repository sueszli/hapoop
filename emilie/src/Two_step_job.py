
import MR_step1 as m1
import MR_step2 as m2

if __name__ == "__main__":
    # TODO: pass the data file to the first map-reduce job
    m1.MRCategoryCount.run()

    # TODO: pass the data file to the second map-reduce job
    m2.MRChiSquared.run()

    # TODO: is the output where we want it to be?

