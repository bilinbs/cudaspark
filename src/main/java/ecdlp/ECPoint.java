package ecdlp;


import java.math.BigInteger;

import utils.MathUtil;

import java.io.*;

public class ECPoint implements Serializable{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    
    private EllipticCurve mother;

    private BigInteger x, y;
    private boolean iszero;

    private ECPoint[] fastcache = null;
    private ECPoint[] cache = null;

    public void fastCache() {
	
        if (fastcache == null) {
            fastcache = new ECPoint[256];
            fastcache[0] = new ECPoint(mother);
            for (int i = 1; i < fastcache.length; i++) {
                fastcache[i] = fastcache[i - 1].add(this);
            }
        }
	 
    }

    /** Constructs a point on an elliptic curve.
     *@param mother The elliptic curve on wich the point is surposed to lie
     *@param x the x coordinate of the point
     *@param y the y coordinate of the point
     *@exception Throws a NotOnMotherException if (x,y) is not on the mother curve.*/
    public ECPoint(EllipticCurve mother, BigInteger x, BigInteger y){
	this.mother = mother;
	this.x = x;
	this.y = y;

	iszero = false;
    }

    /** Decompresses a compressed point stored in a byte-array into a new ECPoint.
     *@param bytes the array of bytes to be decompressed
     *@param mother the EllipticCurve the decompressed point is supposed to lie on.*/
    public ECPoint(byte[] bytes, EllipticCurve mother) {
        this.mother = mother;
        if (bytes[0] == 2) {
            iszero = true;
            return;
        }
        boolean ymt = false;
        if (bytes[0] != 0)
            ymt = true;
        bytes[0] = 0;
        x = new BigInteger(bytes);
        if (mother.getPPODBF() == null)
            System.out.println("Fuck dig!!!");
        y = x.multiply(x).add(mother.geta()).multiply(x).add(mother.getb())
                .modPow(mother.getPPODBF(), mother.getp());
        if (ymt != y.testBit(0)) {
            y = mother.getp().subtract(y);
        }
        iszero = false;
    }
     
    /** IMPORTANT this renders the values of x and y to be null! Use this constructor
     *only to create instances of a Zero class!*/
    public ECPoint(EllipticCurve e){
	x = y = BigInteger.ZERO;
	mother = e;
	iszero = true;
    }

    public byte[] compress() {
        byte[] cmp = new byte[mother.getPCS()];
        if (iszero) {
            cmp[0] = 2;
        }
        byte[] xb = x.toByteArray();
        System.arraycopy(xb, 0, cmp, mother.getPCS() - xb.length, xb.length);
        if (y.testBit(0))
            cmp[0] = 1;
        return cmp;
    }

    /** Adds another elliptic curve point to this point.
     *@param q The point to be added
     *@return the sum of this point on the argument
     *@exception Throws a NoCommonMotherException if the two points don't lie on the same elliptic curve.*/
    public ECPoint add(ECPoint q) {

        if (this.iszero)
            return q;
        else if (q.isZero())
            return this;

        BigInteger y1 = y;
        BigInteger y2 = q.gety();
        BigInteger x1 = x;
        BigInteger x2 = q.getx();

        BigInteger alpha;

        if (x2.compareTo(x1) == 0) {

            if (!(y2.compareTo(y1) == 0))
                return new ECPoint(mother);
            else {
                alpha = ((x1.modPow(MathUtil.TWO, mother.getp())).multiply(MathUtil.THREE))
                        .add(mother.geta());
                alpha = (alpha
                        .multiply((MathUtil.TWO.multiply(y1)).modInverse(mother.getp())))
                                .mod(mother.getp());
            }

        } else {
            alpha = ((y2.subtract(y1))
                    .multiply((x2.subtract(x1)).modInverse(mother.getp())))
                            .mod(mother.getp());
        }

        BigInteger x3, y3;
        x3 = (((alpha.modPow(MathUtil.TWO, mother.getp())).subtract(x2)).subtract(x1))
                .mod(mother.getp());
        y3 = ((alpha.multiply(x1.subtract(x3))).subtract(y1))
                .mod(mother.getp());

        return new ECPoint(mother, x3, y3);

    }
    
	public ECPoint multiply(BigInteger coef) {

		ECPoint result = new ECPoint(mother);
		byte[] coefb = coef.toByteArray();
		if (fastcache != null) {
			for (int i = 0; i < coefb.length; i++) {
				result = result.times256().add(fastcache[coefb[i] & 255]);
			}
			return result;
		}
		if (cache == null) {
			cache = new ECPoint[16];
			cache[0] = new ECPoint(mother);
			for (int i = 1; i < cache.length; i++) {
				cache[i] = cache[i - 1].add(this);
			}
		}
		for (int i = 0; i < coefb.length; i++) {
			result = result.times16().add(cache[(coefb[i] >> 4) & 15]).times16().add(cache[coefb[i] & 15]);
		}
		return result;

	}

	public ECPoint halve() {
	    
	    BigInteger m = getx().add(mother.geta());
	    return this;
	}
	
	private ECPoint times16() {
		try {
			ECPoint result = this;
			for (int i = 0; i < 4; i++) {
				result = result.add(result);
			}
			return result;
		} catch (Exception e) {
			System.out.println("ECPoint.times16: THIS CANNOT HAPPEN!!!");
			return null;
		}
	}

	private ECPoint times256() {
		try {
			ECPoint result = this;
			for (int i = 0; i < 8; i++) {
				result = result.add(result);
			}
			return result;
		} catch (Exception e) {
			System.out.println("ECPoint.times256: THIS CANNOT HAPPEN!!!");
			return null;
		}
	}

    public BigInteger getx(){
	return x;
    }

    public BigInteger gety(){
	return y;
    }

    public EllipticCurve getMother(){
	return mother;
    }

    public String toString(){
	return "(" + x.toString() + ", " + y.toString() + ")";
    }

    public boolean hasCommonMother(ECPoint p){
	if (this.mother.equals(p.getMother())) return true;
	else return false;
    }

    public boolean isZero(){
	return iszero;
    }
    
    public int weight(){
    	return x.bitCount()+y.bitCount();
    }
    
    public boolean isDistinguished(){
    	
    	return true;
    }
    
    public int inGroup(){
    	
    	return 1;
    }
}
