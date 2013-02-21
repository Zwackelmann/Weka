package weka.core.matrix;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;

public class MemorySavingMatrix {
    private PageManager pageManager;
    private int cols;
    private int rows;
    private int colCount = 0;
    public double[] diag;
    
    public MemorySavingMatrix(int rows, int cols) {
        this.rows = rows;
        this.cols = cols;
        diag = new double[Math.min(rows, cols)];
        pageManager = new PageManager(cols);
    }
    
    public void addColumn(double[] colValues) {
        if(colCount < colValues.length) {
            diag[colCount] = colValues[colCount];
            colCount++;
        }
        
        pageManager.addColumn(colValues);
    }
    
    public double[] aquireColumn(int colNr) {
        return pageManager.aquireColumn(colNr);
    }
    
    public void unlockColumn(int colNr) {
        pageManager.unlockColumn(colNr);
    }
    
    public void setValue(int row, int col, double value) {
        double[] targetCol = aquireColumn(col);
        targetCol[row] = value;
        unlockColumn(col);
    }
    
    public double getValue(int row, int col) {
        double[] targetColumn = aquireColumn(col);
        double value = targetColumn[row];
        unlockColumn(col);
        return value;
    }
    
    public int getNumRows() {
        return rows;
    }
    
    public int getNumCols() {
        return cols;
    }
    
    public SingularValueDecomposition svd() {
        return new SingularValueDecomposition(this);
    }
    
    private static class PageManager {
        private static final int pageSize = 10;
        private static final int maxCachedPages = 5;
        
        double[][] currentlySavedPage = new double[pageSize][];
        int colCount = 0;
        int maxColumns;
        
        LinkedList<Page> pages = new LinkedList<Page>();
        
        public PageManager(int maxColumns) {
            this.maxColumns = maxColumns;
        }
        
        public void addColumn(double[] col) {
            currentlySavedPage[colCount % pageSize] = col;
            
            if(colCount % pageSize == pageSize-1) {
                Page page = new Page(colCount - pageSize + 1, colCount, currentlySavedPage);
                page.sourceOut();
                pages.add(page);
                
                currentlySavedPage = new double[pageSize][];
            }
            if(colCount+1 == maxColumns) {
                int lastPageSize = (colCount+1) % pageSize;
                
                if(lastPageSize != 0) {
                    Page page = new Page(colCount - lastPageSize + 1, colCount, currentlySavedPage);
                    page.sourceOut();
                    pages.add(page);
                }
                System.out.println("Matrix full");
            }
            colCount++;
        }
        
        public Page pageByColumn(int colNr) {
            for(Page page : pages) {
                if(page.hasCol(colNr)) {
                    return page;
                }
            }
            throw new RuntimeException("Col " + colNr + " is not supported by any page available.");
        }
        
        public double[] aquireColumn(int colNr) {
            Page targetPage = pageByColumn(colNr);
            
            if(targetPage.dataCurrentlyLoaded) {
                return targetPage.aquireColumn(colNr);
            } else {
                if(numPagesWithLoadedData() > maxCachedPages) {
                    Page lfuPage = getLfuPage();
                    lfuPage.sourceOut();
                }
                targetPage.reload();
                return targetPage.aquireColumn(colNr);
            }
        }
        
        public void unlockColumn(int colNr) {
            pageByColumn(colNr).unlockColumn(colNr);
        }
        
        public int numPagesWithLoadedData() {
            int pagesWithLoadedData = 0;
            for(Page page : pages) {
                if(page.dataCurrentlyLoaded) {
                    pagesWithLoadedData++;
                }
            }
            return pagesWithLoadedData;
        }
        
        public Page getLfuPage() {
            Page lfuPage = null;
            
            for(Page page : pages) {
                if(!page.isLocked() && page.dataCurrentlyLoaded) {
                    if(lfuPage == null) {
                        lfuPage = page;
                    } else if(page.recentAccesses() < lfuPage.recentAccesses()) {
                        lfuPage = page;
                    }
                }
            }
            
            if(lfuPage != null) {
                return lfuPage;
            } else {
                throw new RuntimeException("All Pages in cache are locked and the page limit is reached.");
            }
        }
    }
    
    private static class Page {
        public static final int recent = 3600000;
        int colFrom;
        int colTo;
        
        LinkedList<Long> accesses;
        double[][] data;
        int[] locks;
        boolean dataCurrentlyLoaded;
        
        public Page(int colFrom, int colTo, double[][] data) {
            System.out.println("new page from " + colFrom + " to " + colTo);
            this.colFrom = colFrom;
            this.colTo = colTo;
            this.data = data;
            
            accesses = new LinkedList<Long>();
            dataCurrentlyLoaded = true;
            locks = new int[colTo-colFrom+1];
            for(int i=0; i<(colTo-colFrom); i++) {
                locks[i] = 0;
            }
        }
        
        public void sourceOut() {
            System.out.println("sourceOutPage");
            ObjectOutputStream outputStream = null;
            
            try {
                outputStream = new ObjectOutputStream(new FileOutputStream("cache/" + this.hashCode()));
                outputStream.writeObject(data);
                data = null;
                dataCurrentlyLoaded = false;
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (outputStream != null) {
                        outputStream.flush();
                        outputStream.close();
                    }
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
        
        public void reload() {
            System.out.println("reload page");
            ObjectInputStream inputStream = null;
            try {
                inputStream = new ObjectInputStream(new FileInputStream("cache/" + this.hashCode()));
                data = ((double[][])(inputStream.readObject()));
                inputStream.close();
                dataCurrentlyLoaded = true;
            } catch (Exception ex) {
                ex.printStackTrace();
                try {
                    inputStream.close();
                } catch (Exception ex2){
                    ex2.printStackTrace();
                }
            }
        }
        
        public boolean hasCol(int colNr) {
            return (colNr >= colFrom && colNr <= colTo);
        }
        
        public double[] aquireColumn(int colNr) {
            if(!hasCol(colNr)) {
                throw new RuntimeException("ColNr " + colNr + " is not within bounds of this page.");
            } else if(!dataCurrentlyLoaded) {
                throw new RuntimeException("This Pages data is currently not loaded.");
            } else {
                access();
                locks[colNr - colFrom]++;
                return data[colNr - colFrom];
            }
        }
        
        public void unlockColumn(int colNr) {
            locks[colNr - colFrom]--;
        }
        
        public boolean isLocked() {
            for(int lockCount : locks) {
                if(lockCount != 0) {
                    return true;
                }
            }
            return false;
        }
        
        public void access() {
            accesses.add(System.currentTimeMillis());
        }
        
        public int recentAccesses() {
            long currentTime = System.currentTimeMillis();
            
            while(!accesses.isEmpty() && (currentTime - accesses.getFirst()) > recent ) {
                accesses.removeFirst();
            }
            
            return accesses.size();
        }
    }
} 






