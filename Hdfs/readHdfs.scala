def readHdfsFile(filePath:String) : Option[String] = {
    try{
      val path = new Path(filePath)
      val stream = fs.open(path)
      def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))
      val content : String = readLines.takeWhile(_ != null).mkString(configFlags.NEWLINE)
      logger.info("Loaded the data from HDFS successfully")
      Some(content)
    }catch {
      case npe: NullPointerException =>
        logger.info(npe.printStackTrace())
        None
      case ie: IOException =>
        logger.info(ie.printStackTrace())
        None
      case e: Exception =>
        logger.info(e.printStackTrace())
        None
    }
  }
