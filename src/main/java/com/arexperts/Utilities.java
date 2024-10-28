package com.arexperts;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;

public class Utilities {
    public static int sizeof(Object obj) {

        int length = 0;

        try 
        {
            ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();

            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
    
            objectOutputStream.writeObject(obj);
            objectOutputStream.flush();
            objectOutputStream.close();
    
            length = byteOutputStream.toByteArray().length;
        }
        catch (IOException ex) {

        }
    
        return length;
    }
}