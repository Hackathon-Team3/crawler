//
// Stewards: rajiv (rajiv@maginatics.com)
//
// See
// https://docs.google.com/document/d/1LCQLUAkbbzeJzudXMT88zLS9UCEgqtV1RzqUHPg_ceo/pub
// for example data.
//

#include <iostream>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <curl/curl.h>
#include <boost/property_tree/info_parser.hpp>

/* Standard C++ includes */
#include <stdlib.h>
#include <iostream>

/*
  Include directly the different
  headers from cppconn/ and mysql_driver.h + mysql_util.h
  (and mysql_connection.h). This will reduce your build time!
*/
#include "mysql-connector-c++-1.1.4-linux-debian6.0-x86-64bit/include/mysql_connection.h"
#include "mysql-connector-c++-1.1.4-linux-debian6.0-x86-64bit/include/mysql_driver.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

using namespace std;
using std::cout;

void updateDB(std::string const& url, std::string const& mdata) {
  // using namespace sql::mysql;

  try {
    sql::Driver *driver;
    sql::Connection *con;
    sql::PreparedStatement *stmt;

    /* Create a connection */
    driver = get_driver_instance();
    con = driver->connect("tcp://hackathon-marsrovers.cgaxek1tv5pz.us-west-1.rds.amazonaws.com:3306",
			  "roveruser", "wqeindsa235");
    /* Connect to the MySQL test database */
    con->setSchema("hackathon_marsrovers");

    stmt = con->prepareStatement("INSERT INTO images(URL, metadata) VALUES (?, ?)");
    stmt->setString(1, url);
    stmt->setString(2, mdata);
    stmt->executeUpdate();
    delete stmt;
    delete con;
  } catch (sql::SQLException &e) {
    cout << "# ERR: SQLException in " << __FILE__;
    cout << "(" << __FUNCTION__ << ") on line "
	 << __LINE__ << endl;
    cout << "# ERR: " << e.what();
    cout << " (MySQL error code: " << e.getErrorCode();
    cout << ", SQLState: " << e.getSQLState() << " )" << endl;
  }
}

size_t AppendDataToStringCurlCallback(void *ptr, size_t size, size_t nmemb,
                                      void *vstring) {
    std::string * pstring = (std::string*)vstring;
    pstring->append((char*)ptr, size * nmemb);
    return size * nmemb;
}

std::string DownloadUrlAsString(const std::string & url) {
    std::string body;
    CURL *curl_handle;
    curl_global_init(CURL_GLOBAL_ALL);
    curl_handle = curl_easy_init();
    curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION,
                     AppendDataToStringCurlCallback);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &body);
    curl_easy_perform(curl_handle);
    curl_easy_cleanup(curl_handle);

    return body;
}

void processImagesHelper(boost::property_tree::ptree *pt,
                         std::string imageType) {
    if (!pt->get_child_optional(imageType)) {
        cout << "Does not exist: " << imageType << "\n";
        return;
    }

    // Crawl images array
    for (boost::property_tree::ptree::value_type &imgs :
             pt->get_child(imageType)) {
        for (boost::property_tree::ptree::value_type &img :
                 imgs.second.get_child("images.")) {
            std::string url = img.second.get<std::string>("url");
	    write_json("tmp.txt", img.second);
	    std::ifstream t("tmp.txt");
	    std::stringstream buffer;
	    buffer << t.rdbuf();
	    std::string mdata = buffer.str();
            cout << "Img url: " << url << "\n";
	    // cout << "Mdata: " << mdata << "\n";
	    updateDB(url, mdata);
        }
    }
}

void processImages(std::string const& imagesUrl) {
    try {
        boost::property_tree::ptree pt;
        std::stringstream rootUrl;
        rootUrl << DownloadUrlAsString(imagesUrl);
        boost::property_tree::read_json(rootUrl, pt);
        processImagesHelper(&pt, "mi_images.");
        processImagesHelper(&pt, "pcam_images.");
        processImagesHelper(&pt, "ncam_images.");
        processImagesHelper(&pt, "fcam_images.");
        processImagesHelper(&pt, "rcam_images.");
        processImagesHelper(&pt, "ccam_images.");
        processImagesHelper(&pt, "mahli_images.");
        processImagesHelper(&pt, "mardi_images.");
        processImagesHelper(&pt, "mastcam_left_images.");
        processImagesHelper(&pt, "mastcam_right_images.");
        processImagesHelper(&pt, "course_plot_images.");
    } catch (std::exception const& e) {
        std::cout << "Error on parsing marsbook json for images: " << e.what();
    }
}

void processSols(std::string const& solsUrl) {
    try {
        boost::property_tree::ptree pt;
        std::stringstream rootUrl;
        rootUrl << DownloadUrlAsString(solsUrl);
        boost::property_tree::read_json(rootUrl, pt);
        for (boost::property_tree::ptree::value_type &sol :
                 pt.get_child("sols.")) {
            int id = sol.second.get<int>("sol");
            int numImages = sol.second.get<int>("num_images");
            std::string url = sol.second.get<std::string>("url");
            cout << "============ Sol id: " << id << " =============== \n";
            cout << "numImages: " << numImages << "\n";
            cout << "url: " << url << "\n";
            processImages(url);
        }
    } catch (std::exception const& e) {
        std::cout << "Error on parsing marsbook json for sols: " << e.what();
    }
}

int main() {
    int status = 0;
    try {
        boost::property_tree::ptree pt;
        std::stringstream rootUrl;
        rootUrl << DownloadUrlAsString("http://json.jpl.nasa.gov/data.json");
        boost::property_tree::read_json(rootUrl, pt);

        std::string mera = pt.get<std::string>("MERA.image_manifest");
        std::cout << mera << "\n";
        std::string merb = pt.get<std::string>("MERB.image_manifest");
        std::cout << merb << "\n";
        std::string msl = pt.get<std::string>("MSL.image_manifest");
        std::cout << msl << "\n";

	//updateDB("http://my.img.test", "{json}");

        processSols(mera);
        processSols(merb);
        processSols(msl);
    } catch (std::exception const& e) {
        std::cout << "Error on parsing marsbook json : " << e.what();
        status = -1;
    }
    return status;
}
