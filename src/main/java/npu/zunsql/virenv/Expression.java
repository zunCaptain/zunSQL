package npu.zunsql.virenv;

import javafx.util.Pair;
import npu.zunsql.treemng.BasicType;

import java.util.Stack;

public class Expression {
    private Stack<UnionOperand> operands;

    public Expression(){
        operands=new Stack<>();
    }

    public void clearStack(){
        operands.clear();
    }

    public void addOperand(UnionOperand obj){
        operands.add(obj);
    }
    public void applyOperator(OpCode op){
        if(op== OpCode.Neg||op== OpCode.Not){
            UnionOperand a=operands.pop();
            switch (op){
                case Neg:
                    operands.push(new UnionOperand(BasicType.Float,Neg(a).toString()));
                    break;
                case Not:
                    operands.push(new UnionOperand(BasicType.Integer,Not(a).toString()));
                    break;
                default:
                    break;
            }
        }
        else{
            UnionOperand a=operands.pop();
            UnionOperand b=operands.pop();
            switch (op){
                case EQ:
                    operands.push(new UnionOperand(BasicType.Integer,EQ(a,b).toString()));
                    break;
                case GE:
                    operands.push(new UnionOperand(BasicType.Integer,GE(a,b).toString()));
                    break;
                case GT:
                    operands.push(new UnionOperand(BasicType.Integer,GT(a,b).toString()));
                    break;
                case LE:
                    operands.push(new UnionOperand(BasicType.Integer,LE(a,b).toString()));
                    break;
                case LT:
                    operands.push(new UnionOperand(BasicType.Integer,LT(a,b).toString()));
                    break;
                case Sub:
                    operands.push(new UnionOperand(BasicType.Float,Sub(a,b).toString()));
                    break;
                case Mul:
                    operands.push(new UnionOperand(BasicType.Float,Mul(a,b).toString()));
                    break;
                case Div:
                    operands.push(new UnionOperand(BasicType.Float,Div(a,b).toString()));
                    break;
                case Add:
                    Pair<String,Double> ans=Add(a,b);
                    if(ans.getKey()==null){
                        operands.push(new UnionOperand(BasicType.Float,ans.getValue().toString()));
                    }
                    else{
                        operands.push(new UnionOperand(BasicType.String,ans.getKey()));
                    }
                    break;
                case And:
                    operands.push(new UnionOperand(BasicType.Integer,And(a,b).toString()));
                    break;
                case Or:
                    operands.push(new UnionOperand(BasicType.Integer,Or(a,b).toString()));
                    break;
                case NE:
                    operands.push(new UnionOperand(BasicType.Integer,NE(a,b).toString()));
                    break;
                default:
                    break;
            }
        }
    }
    public UnionOperand getAns(){
        return operands.peek();
    }

    Pair<UnionOperand,UnionOperand> typeCast(Pair<UnionOperand,UnionOperand> obj){
        UnionOperand a=obj.getKey();
        UnionOperand b=obj.getValue();

        if(a.getType()!=b.getType()){
            if(a.getType()==BasicType.Integer){
                a=new UnionOperand(b.getType(),a.getValue());
            }
            else if(a.getType()==BasicType.Float){
                if(b.getType()==BasicType.Integer){
                    b=new UnionOperand(BasicType.Float,b.getValue());
                }
                else{
                    a=new UnionOperand(BasicType.String,a.getValue());
                }
            }
            else{
                b=new UnionOperand(BasicType.String,b.getValue());
            }
        }
        return new Pair<>(a,b);
    }
    Integer EQ(UnionOperand a,UnionOperand b){
        Pair<UnionOperand,UnionOperand> obj=typeCast(new Pair<>(a,b));
        if(obj.getKey().getValue().equals(obj.getValue().getValue())){
            return 1;
        }
        else if(obj.getKey().getType() == BasicType.String){
            if(obj.getKey().getValue().equals(obj.getValue().getValue()))
                return 1;
            else return 0;
        }
        else{
            return 0;
        }
    }
    Integer NE(UnionOperand a,UnionOperand b){
        return 1-EQ(a,b);
    }
    Integer GE(UnionOperand a,UnionOperand b){
        Pair<UnionOperand,UnionOperand> obj=typeCast(new Pair<>(a,b));
        if(obj.getKey().getType()==BasicType.String){
            if(obj.getKey().getValue().compareTo(obj.getValue().getValue())<=0){
                return 1;
            }
            else{
                return 0;
            }
        }
        else{
            //int和double都可以使用double类型来比较大小
            Double x=Double.valueOf(obj.getKey().getValue());
            Double y=Double.valueOf(obj.getValue().getValue());
            if(y>=x){
                return 1;
            }
            else{
                return 0;
            }
        }
    }
    Integer GT(UnionOperand a,UnionOperand b){
        Pair<UnionOperand,UnionOperand> obj=typeCast(new Pair<>(a,b));
        if(obj.getKey().getType()==BasicType.String){
            if(obj.getKey().getValue().compareTo(obj.getValue().getValue())<0){
                return 1;
            }
            else{
                return 0;
            }
        }
        else{
            //int和double都可以使用double类型来比较大小
            Double x=Double.valueOf(obj.getKey().getValue());
            Double y=Double.valueOf(obj.getValue().getValue());
            if(y>x){
                return 1;
            }
            else{
                return 0;
            }
        }
    }
    Integer LE(UnionOperand a,UnionOperand b){
        Pair<UnionOperand,UnionOperand> obj=typeCast(new Pair<>(a,b));
        if(obj.getKey().getType()==BasicType.String){
            if(obj.getKey().getValue().compareTo(obj.getValue().getValue())>=0){
                return 1;
            }
            else{
                return 0;
            }
        }
        else{
            //int和double都可以使用double类型来比较大小
            Double x=Double.valueOf(obj.getKey().getValue());
            Double y=Double.valueOf(obj.getValue().getValue());
            if(y<=x){
                return 1;
            }
            else{
                return 0;
            }
        }
    }
    Integer LT(UnionOperand a,UnionOperand b){
        Pair<UnionOperand,UnionOperand> obj=typeCast(new Pair<>(a,b));
        if(obj.getKey().getType()==BasicType.String){
            if(obj.getKey().getValue().compareTo(obj.getValue().getValue())>0){
                return 1;
            }
            else{
                return 0;
            }
        }
        else{
            //int和double都可以使用double类型来比较大小
            Double x=Double.valueOf(obj.getKey().getValue());
            Double y=Double.valueOf(obj.getValue().getValue());
            if(y<x){
                return 1;
            }
            else{
                return 0;
            }
        }
    }
    Pair<String,Double> Add(UnionOperand a,UnionOperand b){
        Pair<UnionOperand,UnionOperand> obj=typeCast(new Pair<>(a,b));
        if(obj.getKey().getType()==BasicType.String){
            return new Pair<>(obj.getKey().getValue().concat(obj.getValue().getValue()),null);
        }
        else{
            //int和double都可以使用double类型来计算
            Double x=Double.valueOf(obj.getKey().getValue());
            Double y=Double.valueOf(obj.getValue().getValue());
            return new Pair<>(null,x+y);
        }
    }
    Double Sub(UnionOperand a,UnionOperand b){
        Pair<UnionOperand,UnionOperand> obj=typeCast(new Pair<>(a,b));
        if(obj.getKey().getType()==BasicType.String){
            Util.log("字符串不能做减法！");
            return 0.0;
        }
        else{
            //int和double都可以使用double类型来计算
            Double x=Double.valueOf(obj.getKey().getValue());
            Double y=Double.valueOf(obj.getValue().getValue());
            return y-x;
        }
    }
    Double Mul(UnionOperand a,UnionOperand b){
        Pair<UnionOperand,UnionOperand> obj=typeCast(new Pair<>(a,b));
        if(obj.getKey().getType()==BasicType.String){
            Util.log("字符串不能做乘法！");
            return 0.0;
        }
        else{
            //int和double都可以使用double类型来计算
            Double x=Double.valueOf(obj.getKey().getValue());
            Double y=Double.valueOf(obj.getValue().getValue());
            return x*y;
        }
    }
    Double Div(UnionOperand a,UnionOperand b){
        Pair<UnionOperand,UnionOperand> obj=typeCast(new Pair<>(a,b));
        if(obj.getKey().getType()==BasicType.String){
            Util.log("字符串不能做除法！");
            return 0.0;
        }
        else{
            //int和double都可以使用double类型来计算
            Double x=Double.valueOf(obj.getKey().getValue());
            Double y=Double.valueOf(obj.getValue().getValue());
            if(Math.abs(y)<1e-10){
                Util.log("除数不能为0");
                return 0.0;
            }
            if(obj.getKey().getType() == BasicType.Integer &&
                    obj.getValue().getType() == BasicType.Integer){
                return (double)(int)(y/x);
            }
            return y/x;
        }
    }
    Integer And(UnionOperand a,UnionOperand b){
        try{
            Integer x=(int)(double)Double.valueOf(a.getValue());
            Integer y=(int)(double)Double.valueOf(b.getValue());
            if(x==0||y==0){
                return 0;
            }
            else{
                return 1;
            }
        }
        catch (Exception e){
            throw e;
        }
        finally {
            //没什么可做的
        }

    }
    Integer Or(UnionOperand a,UnionOperand b){
        try{
            Integer x=(int)(double)Double.valueOf(a.getValue());
            Integer y=(int)(double)Double.valueOf(b.getValue());
            if(x!=0||y!=0){
                return 1;
            }
            else{
                return 0;
            }
        }
        catch (Exception e){
            throw e;
        }
        finally {
            //没什么可做的
        }

    }
    Integer Not(UnionOperand a){
        try{
            Integer x=(int)(double)Double.valueOf(a.getValue());
            return 1-x;
        }
        catch (Exception e){
            throw e;
        }
        finally {
            //没什么可做的
        }
    }
    Double Neg(UnionOperand a){
        try{
            Double x=Double.valueOf(a.getValue());
            return -x;
        }
        catch (Exception e){
            throw e;
        }
        finally {
            //没什么可做的
        }
    }
}
