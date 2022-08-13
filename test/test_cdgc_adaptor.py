from converter.cdgc_adaptor import CDGCAdaptor

if __name__ == '__main__':
    CDGCAdaptor.convert_objects_csv('./test_objects.csv', '../tmp/test/cdgc/objects')
    CDGCAdaptor.convert_links_csv('./test_links.csv', '../tmp/test/cdgc/links')