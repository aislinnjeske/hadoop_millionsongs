package cs455.hadoop.Utilities;

public class Coefficient {
    private double sumHotness;
    private double sumOther;
    private double sumHO;
    private double squareSumH;
    private double squareSumO;
    private double size;

    public void add(double other, double hotness){
        sumOther += other;
        sumHotness += hotness;
        sumHO += other * hotness;
        squareSumO += Math.pow(other, 2);
        squareSumH += Math.pow(hotness, 2);
        size++;
    }

    public double getCoefficient(){
        double corr = (size * sumHO - sumHotness * sumOther) / (Math.sqrt((size * squareSumH - sumHotness * sumHotness) *
                (size * squareSumO - sumOther * sumOther)));
        return corr;
    }
}
