package utils;

import java.math.BigInteger;
import java.math.RoundingMode;

import com.google.common.math.BigIntegerMath;


public class MathUtil {


    public final static BigInteger TWO = new BigInteger("2");
    public final static BigInteger THREE = new BigInteger("3");
    public final static BigInteger FOUR = new BigInteger("4");
    
    public static  BigInteger quadraticEquationRoot1(BigInteger a, BigInteger b, BigInteger c){    
        BigInteger root1, root2; //This is now a double, too.
        
        root1 = (b.negate().add(
                    BigIntegerMath.sqrt(
                                (b.pow(2)).subtract(
                                           MathUtil.FOUR.multiply(a).multiply(c)
                                           )
                                ,RoundingMode.FLOOR)))
                    .divide(MathUtil.TWO.multiply(a));
        root2 = (b.negate().subtract(BigIntegerMath.sqrt((b.pow(2)),RoundingMode.FLOOR)).subtract( a.multiply(c).multiply(MathUtil.FOUR))).divide(a.multiply(MathUtil.TWO));
        return root1.min(root2);  
    }

}
