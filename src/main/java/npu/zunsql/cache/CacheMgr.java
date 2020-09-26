package npu.zunsql.cache;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//管理存储页面类
public class CacheMgr {
	protected static final int CacheCapacity = 10;
	protected String dbName = null;
	//文件头为1024固定值大小
	protected static final int FILEHEADERSIZE = 1024;
	//数据存储页为1024固定值大小
	protected static final int UNUSEDLISTSIZE = 1024;

	//记录处于缓存区的页面
	protected List<Page> cacheList = null;
	//记录出于缓存区的页面，较上处增加了页面id索引
	protected ConcurrentMap<Integer, Page> cachePageMap = null;
	//事务管理，特定id事务包含多个子事务实例
	protected Map<Integer, Transaction> transMgr = null;
	//事务写入管理，特定事务id对应已写入的多个页实例
	protected Map<Integer, List<Page>> transOnPage = null;
	// record the number of the block which stores the unusedList_count
	//此处存放已使用数据页id的整型数组
	protected List<Integer> unusedList_PageID = null;
	private ReadWriteLock lock;

//	protected UserTransaction userTrans = null;
	protected Map<Integer, UserTransaction> userTransMgr = null;
//	protected List<Page> userTransPages = null;
	protected List<Integer> userTransList = null;

	public CacheMgr(String dbName) {
		this.dbName = dbName;
		this.cacheList = new ArrayList<Page>();
		this.cachePageMap = new ConcurrentHashMap<Integer, Page>();
		this.transMgr = new HashMap<Integer, Transaction>();
		this.transOnPage = new HashMap<Integer, List<Page>>();
		this.unusedList_PageID = new ArrayList<Integer>();
		//可重入读写锁，建立临界资源，仅单一线程可访问该数据库
		this.lock = new ReentrantReadWriteLock();

//		this.userTrans = null;
		this.userTransMgr = new HashMap<Integer, UserTransaction>();
//		this.userTransPages = null;
		this.userTransList = null;
	}

	public boolean isNew() {
		File db_file = new File(this.dbName);
		/*
		Channel是对I/O操作的封装。
		FileChannel配合着ByteBuffer，将读写的数据缓存到内存中，然后以批量/缓存的方式read/write，
		省去了非批量操作时的重复中间操作，操纵大文件时可以显著提高效率
		*/
		FileChannel fc = null;
		// if db_file has existed,use the API to read the file
		if (db_file.exists()) {
		//文件存在时，对文件的页进行初始化
			RandomAccessFile fin = null;
			try {
				fin = new RandomAccessFile(db_file, "rw");
				fc = fin.getChannel();
				ByteBuffer fileHeader = ByteBuffer.allocate(CacheMgr.FILEHEADERSIZE);
				//从数据库文件读入文件头页（0-1023）
				fc.read(fileHeader, 0);

				//获取数据库文件头的两个标识值
				int version = fileHeader.getInt(0);
				int magicNum = fileHeader.getInt(4);

				//用上述标识创建文件头实例
				FileHeader obj = new FileHeader(version, magicNum);
				//文件头实例有效性判断（标识判断）
				if (obj.isValid()) {
					//从数据库头文件第3个int中获取该数据库已使用页数
					Page.pageCount = fileHeader.getInt(8);
					System.out.println("当前数据库已使用页数为:"+Page.pageCount);

					//从数据库文件的下一页用于数据存储(1024-2047)
					ByteBuffer unusedListBuffer = ByteBuffer.allocate(CacheMgr.UNUSEDLISTSIZE);
					//从数据库读入下一页
					fc.read(unusedListBuffer, CacheMgr.FILEHEADERSIZE);

					/*
					写模式时改为从头写
					limit在读写模式均与原buffer容量相同
						public final Buffer rewind() {
							position = 0;
							mark = -1;
							return this;
							}
					 */
					//将待写入页光标置为开头
					unusedListBuffer.rewind();

					//获取数据存储页的序号
					//？？？unusedListNum为数据页的总个数
					//此处第一个int存储数据页的总个数（position=0）
					int unusedListNum = unusedListBuffer.getInt();

					//？？？此处对容量大小进行遍历，有问题
					//此处应为对已存在的数据页进行遍历，对空闲页进行记录
					for (int i = 0; i < unusedListNum; i++) {
						//？？？此处应为对每一页的存储容量进行判定，故遍历的i于下方容量判定的i含义不同


						//？？？i % 255的意义？
						if (i % 255 == 0) {

							/*
							flip函数解释：
							flip()函数的作用是将写模式转变为读模式，即将写模式下的Buffer中内容的最后位置变为读模式下的limit位置，
							同时将当前读位置置为0，表示转换后重头开始读，同时再消除写模式下的mark标记：
							public final Buffer flip() {
									limit = position;
									position = 0;
									mark = -1;
									return this;
							 }
							 */

							//i=255时该页为空
							unusedListBuffer.flip();
							//？？？整改，将未使用页存储页序号第2个int加入到page变量
							//此处将空页的页序号记录于page类中
							Page.unusedID.add(unusedListBuffer.getInt());

						//此处表面该页完全没有使用，仅存在标识信息（除去255,256）
						} else if (i % 255 == 254) {
							//将该空页序号记录于page类
							//？？此处应对page记录列表进行验证，以避免重复记录
							Page.unusedID.add(unusedListBuffer.getInt());		//？？？此处应为第2个int，导入页序号
							int pageID = unusedListBuffer.getInt();		//？？？此处应为第2个int，导入页序号
							//解释：
							//page中的unusedID包括完全空+部分空页面
							//cachemgr的unusedList_PageID仅包含完全空页面
							//空页序号加入到页面管理抽象类的未使用页列表中
							this.unusedList_PageID.add(pageID);

							//将数据库中的读入新一页读入unusedListBuffer
							fc.read(unusedListBuffer,
									CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + pageID * Page.PAGE_SIZE);
						} else
							//若仍存有容量，则仅加入到page类的未使用页列表中
							Page.unusedID.add(unusedListBuffer.getInt());
					}
					System.out.println("成功加载旧数据库文件");
					return false;
				}
				// version and magic is not right,so delete the file and create a new one
				else {
					db_file.delete();
					db_file.createNewFile();
					fin = new RandomAccessFile(db_file, "rw");
					fc = fin.getChannel();
					fileHeader.rewind();
					fileHeader.putInt(0, 1); // version
					fileHeader.putInt(4, 123); // magic number
					fc.write(fileHeader, 0);
					ByteBuffer unusedListBuffer = ByteBuffer.allocate(CacheMgr.UNUSEDLISTSIZE);
					fc.write(unusedListBuffer, CacheMgr.FILEHEADERSIZE);
					return true;
				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		//文件不存在时新建一个数据库存储文件
		} else {
			try {
				db_file.createNewFile();
				RandomAccessFile fin = null;
				fin = new RandomAccessFile(db_file, "rw");
				fc = fin.getChannel();
				ByteBuffer fileHeader = ByteBuffer.allocate(CacheMgr.FILEHEADERSIZE);
				fileHeader.rewind();
				fileHeader.putInt(0, 1); // version
				fileHeader.putInt(4, 123); // magic number
				fileHeader.rewind();
				System.out.println("新数据库文件已建立：");
				System.out.println("version为"+fileHeader.getInt(0));
				System.out.println("magic为"+fileHeader.getInt(4));
				fc.write(fileHeader, 0);
				//申请第1个数据页
				ByteBuffer unusedListBuffer = ByteBuffer.allocate(CacheMgr.UNUSEDLISTSIZE);
				//将第1个数据页写入数据库文件
				//！！此处数据页未设置标识
				fc.write(unusedListBuffer, CacheMgr.FILEHEADERSIZE);
				fc.close();
				fin.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return true;
		}
		return true;
	}

	//关闭时对数据库文件进行页面检查
	public void close() {
		File db_file = new File(this.dbName);
		FileChannel fc = null;
		RandomAccessFile fin = null;
		try {
			fin = new RandomAccessFile(db_file, "rw");
			fc = fin.getChannel();
			ByteBuffer fileHeader = ByteBuffer.allocate(CacheMgr.FILEHEADERSIZE);
			//打开数据库文件并读入文件头
			fc.read(fileHeader, 0);
			fileHeader.rewind();
			//第3个int记录已开启页面数，用于为下一个将开启的页面赋id
			fileHeader.putInt(8, Page.pageCount);
			//将文件头写入源文件
			fc.write(fileHeader, 0);
			//以上为文件头页更新操作


			//以下为数据页检查操作
			ByteBuffer unusedListBuffer = ByteBuffer.allocate(CacheMgr.UNUSEDLISTSIZE);
			//？？？待修改为未使用容量数
			//此处count代表数据存储页未使用页个数，页数为离散的，存在写入页但之后被清空的情况
			int count = Page.unusedID.size();

			//？？？前后count含义不符
			//？？？此处count表示最后一页（或以后一次更新的页）还剩下多少块可以写
			//表还剩下多少个int，每个int为一个block
			int unused_block_num = count / ((CacheMgr.UNUSEDLISTSIZE / 4) - 2); //减去前两个标识位
			//？？不足一个的int块剩下多少字节
			int remain_block_num = count % ((CacheMgr.UNUSEDLISTSIZE / 4) - 2);

			if (remain_block_num != 0) {
				unused_block_num = unused_block_num + 1;
			} else if (remain_block_num == 0 && unused_block_num == 0) {
				//？？？此处剩余块数置1的用处？
				unused_block_num = 1;
			}

			//？？？此处unused_block_num表示数据页个数，前后含义不负
			//若当前计算的未使用页数小于页面管理类记录的未使用页数，说明最后一页未被记录于page抽象类
			while ((unused_block_num - 1) < this.unusedList_PageID.size()) {
				//将最后一页id加入到page类中
				Page.unusedID.add(this.unusedList_PageID.get(this.unusedList_PageID.size() - 1));
				this.unusedList_PageID.remove(this.unusedList_PageID.size() - 1);
			}

			int signal = 0;
			int history_block = -1;


			for (int i = 0; i < Page.unusedID.size(); i++) {
				if (i % 255 == 0) {
					unusedListBuffer.rewind();
					//在刚分配数据页中的第1个int记录未被使用页总数
					unusedListBuffer.putInt(Page.unusedID.size());
					//在刚分配数据页中的第2个int记录对应序号上的未使用页id
					unusedListBuffer.putInt(Page.unusedID.get(i));

				//若该页面
				} else if (i % 255 == 254 || i == Page.unusedID.size() - 1) {
					int backID;
					if (this.unusedList_PageID.size() != 0) {
						backID = this.unusedList_PageID.get(0);
						this.unusedList_PageID.remove(0);
					} else {
						Page.pageCount = Page.pageCount + 1;
						backID = Page.pageCount;
						this.unusedList_PageID.add(backID);
					}
					unusedListBuffer.putInt(Page.unusedID.get(i));
					unusedListBuffer.putInt(backID);
					if (signal == 0) {
						fc.write(unusedListBuffer, CacheMgr.FILEHEADERSIZE);
						signal = 1;
						history_block = backID;
					} else {
						fc.write(unusedListBuffer,
								CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + history_block * Page.PAGE_SIZE);
						history_block = backID;
					}
				} else {
					unusedListBuffer.putInt(Page.unusedID.get(i));
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * start a new transaction and return transID new a objection and get the lock
	 * record the transMgr
	 **/
	public int beginTransation(String s) {
		Transaction trans = new Transaction(s, lock);
		trans.begin();
		this.transMgr.put(trans.transID, trans);
		return trans.transID;
	}

	public int beginUserTransation() {
		UserTransaction trans = new UserTransaction(lock);
		trans.begin();
//		userTrans = trans;
//		this.userTransPages = new ArrayList<>();
		this.userTransList = new ArrayList<>();
		this.userTransMgr.put(trans.transID, trans);
		return trans.transID;
	}

	/**
	 * commit the transaction and update the cache 1.get the transonPage 2.read the
	 * page which will be changed and record it to the journal 3.if hit cache , then
	 * update it 4.write the new one to the page 5.if not hit,then use LRU to
	 * replace one page
	 */
	public boolean commitTransation(int transID) throws IOException {
		Transaction trans = transMgr.get(transID);
		if (trans.WR) {
			List<Page> writePageList = transOnPage.get(transID);
			File journal_file = new File(Integer.toString(transID) + "-journal");
			File db_file = new File(this.dbName);
			RandomAccessFile fin = new RandomAccessFile(journal_file, "rw");
			try {
				FileChannel fc = fin.getChannel();
				// the journal record the transaction ID and pageContent
				ByteBuffer IDBuffer = ByteBuffer.allocate(Page.PAGE_SIZE + 4);
				if (writePageList != null) {
					Set<Integer> set = new HashSet<Integer>();
					for (int i = 0; i < writePageList.size(); i++) {
						Page copyPage = writePageList.get(i);
						set.add(copyPage.getPageID());
					}
					Iterator<Integer> iter = set.iterator();
					int count = 0;
					while (iter.hasNext()) {
						int number = iter.next();

						// get Page from file , add it to cache list and map
						Page tempPage = getPageFromFile(number);

						// tempPage is the original data , try to write journal
						if (journal_file.exists() && journal_file.isFile()) {
							// out.writeObject(tempPage);
							IDBuffer.clear();
							IDBuffer.putInt(tempPage.pageID);
							for (int j = 0; j < Page.PAGE_SIZE; j = j + 4) {
								IDBuffer.putInt(tempPage.pageBuffer.getInt(j));
							}
							IDBuffer.rewind();
							fc.write(IDBuffer, count * (Page.PAGE_SIZE + 4));
							count = count + 1;
						} else {
							System.out.println("fail to write journal");
						}
					}
//	                    byte [] copy_content = new byte[copyPage.getPageBuffer().limit()];
//	                    copyPage.getPageBuffer().rewind();
//	                    copyPage.getPageBuffer().get(copy_content);
					// System.out.println();
					// System.out.println("This is update file page:"+copy_content);

					// write cache
//	                    tempPage.pageID = copyPage.pageID;
//	                    tempPage.getPageBuffer().rewind();
//	                    tempPage.pageBuffer.put(copy_content, 0, copy_content.length);
//	
//	                    byte [] temp_content = new byte[tempPage.getPageBuffer().limit()];
//	                    tempPage.getPageBuffer().rewind();
//	                    tempPage.getPageBuffer().get(temp_content);
					// System.out.println("This is second file page:"+ temp_content);
					for (int i = 0; i < writePageList.size(); i++) {
						Page copyPage = writePageList.get(i);
						for (int j = 0; j < cacheList.size(); j++) {
							Page jPage = cacheList.get(j);
							if (jPage.pageID == copyPage.pageID) {
								cacheList.remove(j);
								cachePageMap.remove(jPage.pageID);
							}
						}
						this.cacheList.add(copyPage);
						this.cachePageMap.put(copyPage.pageID, copyPage);
						// write directly to the database file
						if (db_file.exists() && db_file.isFile()) {
							this.setPageToFile(copyPage, db_file);
						}
					}
				}
			} finally {
				fin.close();
			}
		}
//        this.transMgr.remove(transID);
//        this.transOnPage.remove(transID);
		trans.commit();

		if (userTransList != null) {
			userTransList.add(transID);
		} else {
//			File journal_file = new File(Integer.toString(transID) + "-journal");
//			journal_file.delete();
		}

		return true;
	}

	public boolean commitUserTransation(int transID) throws IOException {
		UserTransaction trans = userTransMgr.get(transID);

		trans.commit();
		int index = (int) (userTransList.size()) - 1;

//		for (; index >= 0; --index) {
//			File journal_file = new File(Integer.toString(userTransList.get(index)) + "-journal");
//			journal_file.delete();
//		}

		this.userTransList = null;

		return true;
	}

	/**
	 * roll back the transaction lease the lock and do not affect cache
	 **/
	public boolean rollbackTransation(int transID) {
//		System.out.println("This is transID:" + transID);
		Transaction trans = transMgr.get(transID);
		FileChannel fc = null;
		Page tempPage = null;
//		if (trans.WR) {
		if (false) {
			File journal_file = new File(Integer.toString(transID) + "-journal");
//			System.out.println("This is file length:" + journal_file.length());
			int num = (int) (journal_file.length() / 1028);
			int i = 0;
			File db_file = new File(this.dbName);
			try {
				RandomAccessFile fin = new RandomAccessFile(journal_file, "r");
				fc = fin.getChannel();
				FileLock lock = fc.lock(0, Long.MAX_VALUE, true);
				ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
				ByteBuffer tempBuffer_ID = ByteBuffer.allocate(4);
				for (i = 0; i < num; i++) {
					fc.read(tempBuffer_ID, i * 1028);
					fc.read(tempBuffer, i * 1028 + 4);
					int tmp_ID = tempBuffer_ID.getInt(0);
					tempPage = new Page(tmp_ID, tempBuffer);
					Page incachePage = this.cachePageMap.get(tmp_ID);
					if (incachePage != null) {
						incachePage.pageBuffer.put(tempPage.pageBuffer);
					}
					if (db_file.exists() && db_file.isFile()) {
						this.setPageToFile(tempPage, db_file);
					}
					tempBuffer_ID.clear();
					tempBuffer.clear();
				}
				lock.release();
				fin.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {

			}
			// try
			// {
//                while(.available() > 1)
//                {
//                    Page srcPage = (Page)in.readObject();
//                    Page tempPage = this.cachePageMap.get(srcPage.pageID);
//                    //cache hit and update the cache file
//                    if (tempPage != null)
//                    {
//                        tempPage.pageBuffer.put(srcPage.pageBuffer);
//                    }
//                    //at the same time , write this into the database file 
//                    //no matter cache is hit , always write into the database file
//                    if (db_file.exists() && db_file.isFile()) {
//                        this.setPageToFile(srcPage, db_file);
//                    }
//                }
//            }
//            catch(Exception e)
//            {
//                e.printStackTrace();
//            }
//            finally
//            {
//                if(in != null) try {
//                    in.close();
//                }
//                catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
		}
		trans.rollback();
//      this.transMgr.remove(transID);
		return true;
	}

	public boolean rollbackUserTransation(int transID) {

//		System.out.println("This is transID need rollback: " + transID);

		UserTransaction trans = userTransMgr.get(transID);

		int index = (int) (userTransList.size()) - 1;
		for (; index >= 0; --index) {
			rollbackTransation(userTransList.get(index));

//			File journal_file = new File(Integer.toString(userTransList.get(index)) + "-journal");
//			journal_file.delete();
		}

		trans.rollback();
		this.userTransMgr.remove(transID);
		this.userTransList = null;
//		this.userTransPages = null;

		return true;
	}

	/**
	 * use transID to read pageID and return page_copy if this page has been stored
	 * in the cacheList ,then directly return it else try to get it from file: if
	 * cacheList is full , use LRU to replace one page else , add this page into
	 * cacheList
	 **/
	public Page readPage(int transID, int pageID) {

		Page tempPage = null;
		//获取页管理类中指定id的页面
		tempPage = this.cachePageMap.get(pageID);

		//获取特定id事务对应的多个页面
		List<Page> writePageList = transOnPage.get(transID);

		// test if this page has been written by this transaction but not commit
		if (writePageList != null) {
			for (int i = 0; i < writePageList.size(); i++) {
				Page copyPage = writePageList.get(i);
				if (copyPage.pageID == pageID)
					return copyPage;
			}
		}

		// 缓存中不存在该页
		if (tempPage == null) {

			// 判断缓存状态
			if (this.cachePageMap.size() >= CacheMgr.CacheCapacity) {

				//缓存已满时，用（LRU）最久未使用页面置换算法换页
				//获取最早一个页面
				tempPage = this.cacheList.get(0);
				//删除该页面
				this.cacheList.remove(0);
				this.cachePageMap.remove(tempPage.pageID);

				//获取函数输入参数中指定id对应页面
				tempPage = getPageFromFile(pageID);
				//将该页面加入到缓存
				this.cacheList.add(tempPage);
				this.cachePageMap.put(tempPage.pageID, tempPage);
			} else {
				//缓存未满时，直接追加该页面于列表末尾
				tempPage = getPageFromFile(pageID);
				this.cacheList.add(tempPage);
				this.cachePageMap.put(tempPage.pageID, tempPage);
			}

		}
		// 缓存中存在该页
		else {
			// LRU置换算法
			//该页被使用，则将该页放置于末尾，表名该页较新
			for (int j = 0; j < cacheList.size(); j++) {
				Page jPage = cacheList.get(j);
				if (jPage.pageID == tempPage.pageID) {
					//从原位置移除
					cacheList.remove(jPage);
				}
			}
			//追加至末尾
			this.cacheList.add(tempPage);
		}
		return tempPage;
	}

	/**
	 * use transID to get the write queue and add it to writePagelist Attention:one
	 * transaction can write more than one page,so we should use a list to store it
	 **/
	// ？？？此函数没用页面重复判定
	// 可能导致多个相同页面加入同一个事务列表中
	public boolean writePage(int transID, Page tempBuffer) {
		//获取特定id事务的写入列表
		List<Page> writePageList = transOnPage.get(transID);
		if (writePageList == null) {
			//不存在时，创建新的记录列表，列表保存将被写入的page类
			writePageList = new ArrayList<Page>();
			//将新的待写入page加入页列表
			writePageList.add(tempBuffer);
			//将事务id即其存储页列表写入map中
			transOnPage.put(transID, writePageList);
		} else {
			//存在时，加入新的待写入page
			writePageList.add(tempBuffer);
		}

		return true;

//		if (userTransPages != null) {
//			userTransPages.add(tempBuffer);
//		}
	}

	/**
	 * Add the unused pageID into the Page.unusedID list
	 **/
	public void deletePage(int transID, int pageID) {
		//删除某页，在页抽象类中加入被删除的页id，记录到未使用页列表中
		Page.unusedID.add(pageID);
	}


	//将指定页面写入到文件
	private boolean setPageToFile(Page tempPage, File file) {
		FileChannel fc = null;
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			RandomAccessFile fin = new RandomAccessFile(file, "rw");
			fc = fin.getChannel();

			//写入情况时，管道上锁
			FileLock lock = fc.lock();
			tempPage.pageBuffer.rewind();
			//将该页写入到按序号排列的页位置
			//
			fc.write(tempPage.pageBuffer,
					CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + tempPage.pageID * Page.PAGE_SIZE);
			lock.release();
			fin.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fc != null) {
					fc.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return true;
	}


	//从文件中读取页面
	private Page getPageFromFile(int pageID) {
		Page tempPage = null;
		FileChannel fc = null;
		try {
			File file = new File(this.dbName);
			RandomAccessFile fin = new RandomAccessFile(file, "rw");
			fc = fin.getChannel();
			// share lock
			FileLock lock = fc.lock(0, Long.MAX_VALUE, true);
			ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
			fc.read(tempBuffer, CacheMgr.FILEHEADERSIZE + CacheMgr.UNUSEDLISTSIZE + pageID * Page.PAGE_SIZE);
			tempPage = new Page(pageID, tempBuffer);
			lock.release();
			fin.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fc != null) {
					fc.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return tempPage;
	}
}
